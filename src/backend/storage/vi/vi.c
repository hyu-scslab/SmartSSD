#ifdef VERSION_INDEX

#include "postgres.h"

#include "access/tableam.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/catalog.h"
#include "catalog/storage.h"
#include "executor/instrument.h"
#include "lib/binaryheap.h"
#include "miscadmin.h"
#include "pg_trace.h"
#include "pgstat.h"
#include "postmaster/bgwriter.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "storage/smgr.h"
#include "storage/standby.h"
#include "storage/procarray.h"
#include "utils/rel.h"
#include "utils/resowner_private.h"
#include "utils/timestamp.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "storage/vi.h"
#include "storage/vi_bufpage.h"
#include "storage/vi_buf.h"
#include "storage/vi_writer.h"

#include "postmaster/vimanager.h"

#define VI_EMPTY_FD (-2)

const char *index_fname_suffix="i";

#ifdef VERSION_INDEX_PERF
/* in vi.h */
ViPerfContext vi_perf_ctx;
char vi_perf_fname[32] = "vi_perf.data";

uint32 visibility_count[3] = {0, 0, 0};
#endif	/* VERSION_INDEX_PERF */

/*
 * ViExtractAndBuildViCtid
 * 		Extract the necessary information to build a version index ctid from
 * 		page items. Returns the built version index (i.e. ViCtidData).
 */
void
ViExtractAndBuildViCtid(BlockNumber blkno,
						Page page_header,
						OffsetNumber offnum,
						ViCtid vi_ctid)
{
	ItemId item_id;
	HeapTupleHeader tup;

	/* ViCtidData contents */
	TransactionId xmin, xmax;
	uint16 infomask;
	SegmentOffset offset;
	ItemLength len;
	uint8 h_len;
	int bitmaplen;
	bits8 *bits;

	item_id = PageGetItemId(page_header, offnum);

	/* PageGetItem points directly to the tuple within the buffer. */
	tup = (HeapTupleHeader) PageGetItem(page_header, item_id);

	/* [ViCtidData] Xmin, Xmax, infomask on top of CanonoicalTupleData */
	xmin = HeapTupleHeaderGetRawXmin(tup);
	xmax = HeapTupleHeaderGetRawXmax(tup);
	infomask = tup->t_infomask;

	/* [CTID] Calculate the absolute offset within the segment. */
	offset = GetSegmentOffset(blkno, ItemIdGetOffset(item_id));
	
	/* [CTID] Tuple length */
	len = ItemIdGetLength(item_id);

	/* [CTID] Tuple header length, including the paddings */
	h_len = tup->t_hoff;

	/* [CTID] Size of null bitmap */
	bitmaplen = HeapTupleHeaderHasNulls(tup)
		? BITMAPLEN(HeapTupleHeaderGetNatts(tup)) : 0;
	bits = tup->t_bits;

	/*
	 * We are done extracting the necessary info for ViToastViCtid(...)
	 */

	/* Toast the ViCtidData */
	ViToastViCtid(vi_ctid, xmin, xmax, infomask, offset, len, h_len,
				  bits, bitmaplen);
}

/*
 * ViPutViCtid
 * 		Find a slot within the version index file and put the new vi_ctid
 * 		made during an insertion or update into the allocated slot.
 * 	
 * We pin 2 pages, the metadata page (page # 0) and vi page (page # N).
 * The metadata page is a lot likely to be pinned within the buffer since
 * it's used on each modification of the segment-wise version index file.
 * Take a close look into the function ViGetFreeSlot(...)
 */
int
ViPutViCtid(Relation relation,
			BlockNumber blkno,
			Page page_header,
			OffsetNumber offnum,
			ViCtid vi_ctid)
{
	Oid	rel_id, seg_id;
	ViPageId page_id;
	ViPageOffset offset;
	int frame_id;
	ItemId item_id;
	HeapTupleHeader tup;

	LWLock *lock;

	rel_id = relation->rd_id;
	seg_id = blkno / RELSEG_SIZE;
	page_id = VI_INVALID_PAGE_ID;
	offset = VI_INVALID_PAGE_OFFSET;

	item_id = PageGetItemId(page_header, offnum);
	tup = (HeapTupleHeader) PageGetItem(page_header, item_id);

	/* Get the offset and page id to insert the new tuple to */
	ViGetFreeSlot(rel_id, seg_id, &page_id, &offset, vi_ctid->xmin);

	Assert(ViPageIdIsValid(page_id));
	Assert(ViPageOffsetIsValid(offset));

	/*
	 * If a new page is required, it would have been inserted on
	 * ViGetFreeSlot(). Make sure you mark the page dirty after writing the
	 * vi_ctid to the corresponding slot. 
	 */
	frame_id = ViGetPage(rel_id, seg_id, page_id, false);
	lock = ViBufferDescriptorGetContentLock(GetViBufferDescriptor(frame_id));

	LWLockAcquire(lock, LW_EXCLUSIVE);

	/* TODO: Delete, temporary logic until free space manager implemented */
	ViMarkOffsetOccupied(frame_id, offset);

	ViCopyViCtidIntoSlot(frame_id, offset, vi_ctid);
	ViMarkPageDirty(frame_id);

	tup->t_page_id = page_id;
	tup->t_offset = offset;

	LWLockRelease(lock);

	ViReleasePage(frame_id);

	return 0;
}

/*
 * ViPrescreen
 * 		Prescreens a segment's version index file (e.g., 16385.2.vi) and
 * 		creates a per segment index file (e.g. 16385.2.i). Returns a
 * 		file descriptor with the index file opened. Caller is responsible
 * 		for closing the file descriptor after use.
 */
int
ViPrescreen(Oid rel_id,
			Oid seg_id,
			Snapshot snapshot,
			bool reuse)
{
	ViPageId numpages;
	ViBuffer meta_frame_id, frame_id;
	ViPerSegmentMeta meta;
	int fd;
	off_t offset;
	char filename[64];
	uint32 num_ctids;
	TransactionId min_xmin;
#ifdef VERSION_INDEX_DEBUG
	uint32 skipped_pages = 0;
#endif	/* VERSION_INDEX_DEBUG */

	Assert(snapshot != InvalidSnapshot);

	/* For index (i.e., relation.segment.i) file */
	sprintf(filename, "%u.%u.%s", rel_id, seg_id, index_fname_suffix);

	/*
	 * The version index file for the current segment has already been built.
	 * Reuse the corresponding file instead of creating a new one. A different
	 * file descriptor must be opened and passed.
	 */
	if (reuse)
	{
#ifdef VERSION_INDEX_DEBUG
		fprintf(stderr, "[ViPrescreen] (Skip segment: duplicate) rel_id: %u, seg_id %u\n", rel_id, seg_id);
#endif	/* VERSION_INDEX_DEBUG */
		fd = open(filename, O_RDONLY, (mode_t)0600);
		return fd;
	}

	/* Read metadata to figure out the number of pages to speculate */
	meta_frame_id = ViGetPage(rel_id, seg_id, VI_META_PAGE_ID, false);

	meta = (ViPerSegmentMeta) ViBufferGetPage(meta_frame_id);
	numpages = meta->last_page_id;
	min_xmin = meta->min_xmin;

	ViReleasePage(meta_frame_id);

	/*
	 * If the xmax value of the snapshot precedes(XID >= xmax) the minimum xmin
	 * within this segment, we do not need to see this segment. The other
	 * concurrently running transactions listed within the snapshot are
	 * invisible since they are in progress, and this minimum is gauranteed
	 * to be the minimum since other committed(or aborted) transactions have
	 * already happened before setting the minimum xmin value of the segment.
	 * Thus, we only need to check the xmax value.
	 */
	if (TransactionIdPrecedes(snapshot->xmax, min_xmin))
	{
#ifdef VERSION_INDEX_DEBUG
		fprintf(stderr, "[ViPrescreen] (Skip segment: xmax < xmin) rel_id: %u, seg_id %u\n", rel_id, seg_id);
#endif	/* VERSION_INDEX_DEBUG */
		return VI_EMPTY_FD;
	}

	fd = open(filename, O_RDWR | O_CREAT | O_TRUNC, (mode_t)0600);

	num_ctids = 0;
	offset = 0;

	/* The first 4 bytes are used to represent the number of CTIDs */
	offset += pg_pwrite(fd, &num_ctids, sizeof(uint32), offset);
	Assert(offset == 4);

	/*
	 * Per segment prescreening using a vi file. The vi file is built per
	 * segment and it contains a continuous array of ViCtidData on each page,
	 * from page #1 (page #0 is used for metadata). Thus, we read each page
	 * from page #1, until the end of the current file.
	 */
	for (ViPageId page_id = VI_META_PAGE_ID + 1; page_id <= numpages; page_id++)
	{
		uint32 num_ctids_per_page;
		bool skip_page = TransactionIdPrecedes(snapshot->xmax, 
								ViGetMinXminPerGroup(rel_id, seg_id, page_id));

		/* Skip this page since the entire content's not visible */
		if (skip_page)
		{
#ifdef VERSION_INDEX_DEBUG
			//fprintf(stderr, "[ViPrescreen] (Skip page: xmax < xmin) rel_id: %u, seg_id %u, page_id: %u\n", rel_id, seg_id, page_id);
			skipped_pages++;
#endif	/* VERSION_INDEX_DEBUG */
#ifndef VERSION_INDEX_DEBUG
			continue;
#endif	/* !VERSION_INDEX_DEBUG */
		}

		frame_id = ViGetPage(rel_id, seg_id, page_id, false);

		/* Holds a shared lock internally */
		num_ctids_per_page = ViPrescreenAndExportIndexPage(fd, &offset,
														   frame_id, snapshot);
#ifdef VERSION_INDEX_DEBUG
		if (skip_page)
		{
			Assert(num_ctids_per_page == 0);
		}
#endif	/* VERSION_INDEX_DEBUG */

		Assert(num_ctids_per_page <= VI_PER_PAGE);

		num_ctids += num_ctids_per_page;

		ViReleasePage(frame_id);
	}

#ifdef VERSION_INDEX_DEBUG
	if (skipped_pages != 0)
	{
		fprintf(stderr, "[ViPrescreen] (Skip pages: xmax < xmin) rel_id: %u, seg_id %u, skipped: %u\n", rel_id, seg_id, skipped_pages);
	}
#endif	/* VERSION_INDEX_DEBUG */

	/* 
	 * TODO: This condition may already be satisfied by the xmin comparision
	 * logic on top.
	 *
	 * There is no visible version in this segment. Drop it.
	 */
	if (num_ctids == 0)
	{
#ifdef VERSION_INDEX_DEBUG
		fprintf(stderr, "[ViPrescreen] (Skip: none visible) rel_id: %u, seg_id %u\n", rel_id, seg_id);
#endif	/* VERSION_INDEX_DEBUG */
		close(fd);
		return VI_EMPTY_FD;
	}

#ifdef VERSION_INDEX_DEBUG
	fprintf(stderr, "[ViPrescreen] (Create) rel_id: %u, seg_id %u, num_ctids: %u\n", rel_id, seg_id, num_ctids);
#endif	/* VERSION_INDEX_DEBUG */

	/* Overwrite the first 4 bytes that represent the number of CTIDs */
	offset = pg_pwrite(fd, &num_ctids, sizeof(uint32), 0);
	Assert(offset == 4);

	/* NOTE: Moved outside the function for performance measurement */
#if 0
	/* Sync the version index file */
	ret = fsync(fd);
	Assert(ret == 0);
#endif

	return fd;
}

void
ViUpdateXmin(Oid rel_id,
			 Oid seg_id,
			 ViPageId page_id,
			 ViPageOffset offset,
			 TransactionId xmin)
{
	ViBuffer frame_id;
	LWLock *lock;

	Assert(page_id != 0);
	Assert(offset < VI_NUM_SLOTS);

	frame_id = ViGetPage(rel_id, seg_id, page_id, false);
	lock = ViBufferDescriptorGetContentLock(GetViBufferDescriptor(frame_id));

	LWLockAcquire(lock, LW_EXCLUSIVE);
	ViUpdateXminInternal(frame_id, offset, xmin);
	LWLockRelease(lock);

	ViMarkPageDirty(frame_id);

	ViReleasePage(frame_id);
}

void
ViUpdateXmax(Oid rel_id,
			 Oid seg_id,
			 ViPageId page_id,
			 ViPageOffset offset,
			 TransactionId xmax)
{
	ViBuffer frame_id;
	LWLock *lock;

	Assert(page_id != 0);
	Assert(offset < VI_NUM_SLOTS);

	frame_id = ViGetPage(rel_id, seg_id, page_id, false);
	lock = ViBufferDescriptorGetContentLock(GetViBufferDescriptor(frame_id));

	LWLockAcquire(lock, LW_EXCLUSIVE);
	ViUpdateXmaxInternal(frame_id, offset, xmax);
	LWLockRelease(lock);

	ViMarkPageDirty(frame_id);

	ViReleasePage(frame_id);
}

void
ViUpdateSegmentOffset(Oid rel_id,
					  Oid seg_id,
					  ViPageId page_id,
					  ViPageOffset offset,
					  SegmentOffset new_seg_offset)
{
	ViBuffer frame_id;
	LWLock *lock;

	Assert(page_id != 0);
	Assert(offset < VI_NUM_SLOTS);

	frame_id = ViGetPage(rel_id, seg_id, page_id, false);
	lock = ViBufferDescriptorGetContentLock(GetViBufferDescriptor(frame_id));

	LWLockAcquire(lock, LW_EXCLUSIVE);
	ViUpdateSegmentOffsetInternal(frame_id, offset, new_seg_offset);
	LWLockRelease(lock);

	ViMarkPageDirty(frame_id);

	ViReleasePage(frame_id);
}

void
ViUpdateHintBits(Oid rel_id,
				 Oid seg_id,
				 ViPageId page_id,
				 ViPageOffset offset,
				 uint16 infomask)
{
	ViBuffer frame_id;
	LWLock *lock;

	Assert(page_id != 0);
	Assert(offset < VI_NUM_SLOTS);

	frame_id = ViGetPage(rel_id, seg_id, page_id, false);
	lock = ViBufferDescriptorGetContentLock(GetViBufferDescriptor(frame_id));

	LWLockAcquire(lock, LW_EXCLUSIVE);
	ViUpdateHintBitsInternal(frame_id, offset, infomask);
	LWLockRelease(lock);

	ViMarkPageDirty(frame_id);

	ViReleasePage(frame_id);
}

void
ViSetHintBits(Oid rel_id,
			  Oid seg_id,
			  ViPageId page_id,
			  ViPageOffset offset,
			  uint16 infomask)
{
	ViBuffer frame_id;
	LWLock *lock;

	Assert(page_id != 0);
	Assert(offset < VI_NUM_SLOTS);

	frame_id = ViGetPage(rel_id, seg_id, page_id, false);
	lock = ViBufferDescriptorGetContentLock(GetViBufferDescriptor(frame_id));

	LWLockAcquire(lock, LW_EXCLUSIVE);
	ViSetHintBitsInternal(frame_id, offset, infomask);
	LWLockRelease(lock);

	ViMarkPageDirty(frame_id);

	ViReleasePage(frame_id);
}

void
ViFreeOffset(Oid rel_id,
			 Oid seg_id,
			 ViPageId page_id,
			 ViPageOffset offset)
{
	ViBuffer frame_id;
	LWLock *lock;

	Assert(page_id != 0);
	Assert(offset < VI_NUM_SLOTS);

	frame_id = ViGetPage(rel_id, seg_id, page_id, false);
	lock = ViBufferDescriptorGetContentLock(GetViBufferDescriptor(frame_id));

	/* Think wisely, as this exclusive lock may not be necessary. */
	LWLockAcquire(lock, LW_EXCLUSIVE);
	Assert(ViPageOffsetIsOccupied(frame_id, offset));
	ViMarkOffsetFree(frame_id, offset);
	LWLockRelease(lock);

	ViMarkPageDirty(frame_id);

	ViReleasePage(frame_id);
}

bool
ViCheckVisibility(TransactionId xmin,
				  TransactionId xmax,
				  uint16 infomask,
				  Snapshot snapshot)
{
	HeapTupleHeaderData header;
	HeapTupleHeader tuple = &header;
	HeapTupleHeaderSetXmin(tuple, xmin);
	HeapTupleHeaderSetXmax(tuple, xmax);
	tuple->t_infomask = infomask;

	if (!HeapTupleHeaderXminCommitted(tuple))
	{
		if (HeapTupleHeaderXminInvalid(tuple))
			return false;

		if (XidInMVCCSnapshot(HeapTupleHeaderGetRawXmin(tuple), snapshot))
			return false;
		else if (TransactionIdDidCommit(HeapTupleHeaderGetRawXmin(tuple)))
		{
			/* The hint bit is unset, but the transaction committed. */
		}
		else
			return false;
	}
	else
	{
		/* xmin is committed, but maybe not according to our snapshot */
		if (!HeapTupleHeaderXminFrozen(tuple) &&
			XidInMVCCSnapshot(HeapTupleHeaderGetRawXmin(tuple), snapshot))
			return false;		/* treat as still in progress */
	}

	/* by here, the inserting transaction has committed */

	if (tuple->t_infomask & HEAP_XMAX_INVALID)	/* xid invalid or aborted */
		return true;

	if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
		return true;

	if (!(tuple->t_infomask & HEAP_XMAX_COMMITTED))
	{
		if (XidInMVCCSnapshot(HeapTupleHeaderGetRawXmax(tuple), snapshot))
			return true;

		if (!TransactionIdDidCommit(HeapTupleHeaderGetRawXmax(tuple)))
		{
			/* it must have aborted or crashed */
			return true;
		}

		/* xmax transaction committed */
	}
	else
	{
		/* xmax is committed, but maybe not according to our snapshot */
		if (XidInMVCCSnapshot(HeapTupleHeaderGetRawXmax(tuple), snapshot))
			return true;		/* treat as still in progress */
	}

	/* xmax transaction committed */

	return false;
}

#ifdef VERSION_INDEX_DEBUG
TransactionId
ViGetXmin(Oid rel_id,
		  Oid seg_id,
		  ViPageId page_id,
		  ViPageOffset offset)
{
	TransactionId xmin;
	ViBuffer frame_id;
	ViPage page;

	frame_id = ViGetPage(rel_id, seg_id, page_id, false);
	page = (ViPage) ViBufferGetPage(frame_id);

	xmin = ViPageGetXmin(page, offset);

	ViReleasePage(frame_id);

	return xmin;
}

TransactionId
ViGetXmax(Oid rel_id,
		  Oid seg_id,
		  ViPageId page_id,
		  ViPageOffset offset)
{
	TransactionId xmax;
	ViBuffer frame_id;
	ViPage page;

	frame_id = ViGetPage(rel_id, seg_id, page_id, false);
	page = (ViPage) ViBufferGetPage(frame_id);

	xmax = ViPageGetXmax(page, offset);

	ViReleasePage(frame_id);

	return xmax;
}

bool
ViDebugCtidEquals(char* CTID,
				  TransactionId xmin,
				  TransactionId xmax,
				  Oid rel_id,
				  Oid seg_id,
				  ViPageId page_id,
				  ViPageOffset offset)
{
	ViBuffer frame_id;
	ViPage page;
	bool ret;

	Ctid ctid = (Ctid) &CTID[0];
	TransactionId vi_xmax, vi_xmin;

	frame_id = ViGetPage(rel_id, seg_id, page_id, false);
	page = (ViPage) ViBufferGetPage(frame_id);

	vi_xmin = ViPageGetXmin(page, offset);
	vi_xmax = ViPageGetXmax(page, offset);

#ifdef VERSION_INDEX_DEBUG
	if (vi_xmin != xmin)
	{
		elog(LOG, "xmin different: %u, %u", vi_xmin, xmin);
	}

	if (vi_xmax != xmax)
	{
		elog(LOG, "xmax different: %u, %u", vi_xmax, xmax);
	}

	if (ctid->offset != page->vi_ctids[offset].ctid.offset)
	{
		elog(LOG, "offset different: %u, %u", ctid->offset, page->vi_ctids[offset].ctid.offset);
	}

	if (ctid->len != page->vi_ctids[offset].ctid.len)
	{
		elog(LOG, "version length different: %u, %u", ctid->len, page->vi_ctids[offset].ctid.len);
	}

	if (ctid->h_len != page->vi_ctids[offset].ctid.h_len)
	{
		elog(LOG, "header length different: %u, %u", ctid->h_len, page->vi_ctids[offset].ctid.h_len);
	}

	if (ctid->bits != page->vi_ctids[offset].ctid.bits)
	{
		elog(LOG, "nullbits different: %u, %u", ctid->bits, page->vi_ctids[offset].ctid.bits);
	}
#endif	/* VERSION_INDEX_DEBUG */

	ret = ((vi_xmin == xmin) && (vi_xmax == xmax) &&
		(ctid->offset == page->vi_ctids[offset].ctid.offset) &&
		(ctid->len == page->vi_ctids[offset].ctid.len) &&
		(ctid->h_len == page->vi_ctids[offset].ctid.h_len) &&
		(ctid->bits == page->vi_ctids[offset].ctid.bits));

	ViReleasePage(frame_id);

	return ret;
}

#endif /* VERSION_INDEX_DEBUG */

#endif /* VERSION_INDEX */
