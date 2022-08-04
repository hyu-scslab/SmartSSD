/*-------------------------------------------------------------------------
 *
 * vi_writer.c
 *	  Operations related to writing version index to vi file.
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/storage/vi/vi_writer.c
 *
 *-------------------------------------------------------------------------
 */
#ifdef VERSION_INDEX

#include "postgres.h"

#include "port/atomics.h"
#include "storage/itemid.h"
#include "storage/block.h"
#include "storage/lwlock.h"
#include "storage/bufpage.h"
#include "storage/buf.h"
#include "access/htup.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "postmaster/vimanager.h"
#include "storage/vi.h"
#include "storage/vi_bufpage.h"
#include "storage/vi_buf.h"
#include "storage/vi_writer.h"

/* bitmap utils */
#define MAX_TUPLE_LENGTH (0x3FFFFFFFU)
#define IsTupleLengthValid(len) (len <= MAX_TUPLE_LENGTH)
#define MAX_NULL_ATTRIBUTES (23)
#define IsNullAttributesValid(bitmaplen) \
	(bitmaplen <= (MAX_NULL_ATTRIBUTES + 7) / 8)
#define VI_BITMAP_MASK (0x0007FFFFU)

#define IsSegmentOffsetValid(seg) ((seg) <= MAX_SEGMENT_OFFSET)

/* Number of pages to allocate in one page expansion */
#define VI_NUM_PAGE_ALLOC (1)

#ifdef VERSION_INDEX_DEBUG
static bool ViCtidIsValid(Ctid ctid);
#endif	/* VERSION_INDEX_DEBUG */

/*
 * ViGetFreeSlot
 * 		Assign a free slot among the version index file (i.e., .vi file). If a
 * 		new page is required, allocate one and mark the page dirty. We currently
 * 		use an exclusive lock to protect the access to the meta page. The page
 * 		id and the allocated offset within the page is set in the parameter.
 *
 * 	NOTE: If we change the allocation logic, we must consider the page number
 * 	checking assertion again. It will have to change to a different logic since
 * 	most likely, the logic will change into a concurrent allocation method and
 * 	multiple new pages might be allocated concurrently.
 */
void
ViGetFreeSlot(Oid rel_id,
			  Oid seg_id,
			  ViPageId *page_id,
			  ViPageOffset *offset,
			  TransactionId xmin)
{
	ViBuffer meta_frame_id, new_frame_id;
	ViPerSegmentMeta meta_page;
	ViPageOffset off;
	ViPageId page;
	LWLock *meta_lock;

	/* Fetch the meta page for identifying if a new page is required */
	meta_frame_id = ViGetPage(rel_id, seg_id, VI_META_PAGE_ID, false);
	meta_page = (ViPerSegmentMeta) ViBufferGetPage(meta_frame_id);
	meta_lock = ViBufferDescriptorGetContentLock(
					GetViBufferDescriptor(meta_frame_id));

	Assert(!ViPageIdIsValid(*page_id));
	Assert(!ViPageOffsetIsValid(*offset));

	LWLockAcquire(meta_lock, LW_EXCLUSIVE);

	off = meta_page->last_slot++;
	page = off / VI_PER_PAGE + 1;	/* version index starts from page #1 */

	if (page > meta_page->last_page_id)
	{
		/* Safe since one access is allowed at a time for allocating a slot */
		Assert(page == meta_page->last_page_id + 1);

		/* Allocate a new page by marking it dirty */
		new_frame_id = ViGetPage(rel_id, seg_id, page, true);
		ViMarkPageDirty(new_frame_id);
		ViReleasePage(new_frame_id);

		meta_page->last_page_id = page;
	}

	/*
	 * We keep track of the minimum xmin to skip the segments that do not
	 * need a rebuild of version index due to obvious visibility.
	 */
	if (TransactionIdIsNormal(xmin) && (xmin < meta_page->min_xmin))
	{
		meta_page->min_xmin = xmin;
	}

	ViSetMinXminPerGroup(rel_id, seg_id, page, xmin);

	LWLockRelease(meta_lock);	

	*page_id = page;
	*offset = off % VI_PER_PAGE;	/* offset within the page */

	ViMarkPageDirty(meta_frame_id);

	ViReleasePage(meta_frame_id);
}

/*
 * ViToastViCtid
 * 		Using the required information, build a ViCtid that contains both the
 * 		xmin, xmax values and the CanoncialTupleData.
 *
 * 	NOTE: The nullbit copy logic may change in a close future.
 */
void
ViToastViCtid(ViCtid vi_ctid,
			  TransactionId xmin, TransactionId xmax, uint16 infomask,
			  SegmentOffset offset, ItemLength len, uint8 h_len,
			  bits8 *bits, int bitmaplen)
{
	unsigned bitpack = 0;

	Assert(IsSegmentOffsetValid(offset));
	Assert(IsTupleLengthValid(len));

	/*
	 * Current maximum number of null attributes in CTID is 23. Thus, the
	 * number of bytes to represent it will be at most 3.
	 */
	Assert(IsNullAttributesValid(bitmaplen));

	vi_ctid->xmin		 = xmin;
	vi_ctid->xmax		 = xmax;
	vi_ctid->infomask 	 = infomask;
	vi_ctid->ctid.offset = offset;
	vi_ctid->ctid.len	 = len;
	vi_ctid->ctid.h_len	 = h_len;

	if (bitmaplen > 0)
	{
		memcpy(&bitpack, bits, bitmaplen);

		Assert(bitpack < 0x000FFFFFU);
	}
	else
	{
		/* All filled with 1 to indicate that every attribute exists */
		bitpack = 0x7FFFFFU;
	}

	vi_ctid->ctid.bits = bitpack;

	Assert(CtidOffsetIsValid(vi_ctid->ctid.offset));
	Assert(CtidLengthIsValid(vi_ctid->ctid.len));
	Assert(CtidHeaderLengthIsValid(vi_ctid->ctid.h_len));
	Assert(CtidBitsIsValid(vi_ctid->ctid.bits));
}

/*
 * ViCopyViCtidIntoSlot
 * 		Copies the ViCtid into the slot. Must hold a pin to the buffer when
 * 		calling this function.
 */
void
ViCopyViCtidIntoSlot(ViBuffer frame_id,
					 ViPageOffset offset,
					 ViCtid vi_ctid)
{
	ViPage page;

	Assert(offset < VI_PER_PAGE);

	page = (ViPage) ViBufferGetPage(frame_id);

	memcpy(&page->vi_ctids[offset], vi_ctid, sizeof(ViCtidData));
}

/*
 * ViPrescreenAndExportIndexPage
 * 		Extract the ViCtids from the page and save it to the CtidChunk.
 * 		Each call handles a maximum of 7 CtidData. The caller must
 * 		hold a pin to the buffer before calling this function.
 */
uint32
ViPrescreenAndExportIndexPage(int fd,
							  off_t *offset,
							  ViBuffer frame_id,
							  Snapshot snapshot)
{
	ViPage page;
	ViPageOffset cursor, chunk_cursor;
	CtidChunkData ctid_chunk;
	ViCtid vi_ctid;
	bool is_visible;
	uint32 num_ctids_per_page = 0;
	LWLock* lock;
#ifdef VERSION_INDEX_PERF
	struct timespec start, stop;
#endif	/* VERSION_INDEX_PERF */

	Assert(fd >= 0);

	page = (ViPage) ViBufferGetPage(frame_id);
	lock = ViBufferDescriptorGetContentLock(GetViBufferDescriptor(frame_id));

	cursor = 0;			/* cursor for offset among the page ViCtids */
	chunk_cursor = 0;	/* cursor for the chunk of ViCtids (0~6) */

	MemSet(&ctid_chunk, 0, sizeof(CtidChunkData));

	LWLockAcquire(lock, LW_SHARED);

	while (cursor < VI_PER_PAGE)
	{
		if (!ViPageOffsetIsOccupied(frame_id, cursor))
		{
			cursor++;
			continue;
		}

		vi_ctid = &page->vi_ctids[cursor];

#ifdef VERSION_INDEX_DEBUG
		Assert(ViCtidIsValid(&vi_ctid->ctid));
#endif	/* VERSION_INDEX_DEBUG */

#ifdef VERSION_INDEX_PERF
		clock_gettime(CLOCK_MONOTONIC, &start);
#endif	/* VERSION_INDEX_PERF */

		is_visible = ViCheckVisibility(vi_ctid->xmin, vi_ctid->xmax,
									   vi_ctid->infomask, snapshot);

#ifdef VERSION_INDEX_PERF
		clock_gettime(CLOCK_MONOTONIC, &stop);
		vi_perf_ctx.visibility_check +=
			(TIMESPEC_TO_MS(stop) - TIMESPEC_TO_MS(start));
#endif	/* VERSION_INDEX_PERF */

		if (is_visible)
		{
#ifdef VERSION_INDEX_PERF
			clock_gettime(CLOCK_MONOTONIC, &start);
#endif	/* VERSION_INDEX_PERF */
			/* Copy the ViCtid's CtidData into the chunk's cursor */
			memcpy(&ctid_chunk.ctids[chunk_cursor], &vi_ctid->ctid,
				   sizeof(CtidData));
#ifdef VERSION_INDEX_PERF
			clock_gettime(CLOCK_MONOTONIC, &stop);
			vi_perf_ctx.memcpy_time +=
				(TIMESPEC_TO_MS(stop) - TIMESPEC_TO_MS(start));
#endif	/* VERSION_INDEX_PERF */
			chunk_cursor++;
		}

		if (chunk_cursor == CtidsPerChunk)
		{
#ifdef VERSION_INDEX_PERF
			clock_gettime(CLOCK_MONOTONIC, &start);
#endif	/* VERSION_INDEX_PERF */
			/* Write out the chunk to index file */
			*offset +=
				pg_pwrite(fd, &ctid_chunk, sizeof(CtidChunkData), *offset);
#ifdef VERSION_INDEX_PERF
			clock_gettime(CLOCK_MONOTONIC, &stop);
			vi_perf_ctx.pwrite_time +=
				(TIMESPEC_TO_MS(stop) - TIMESPEC_TO_MS(start));
#endif	/* VERSION_INDEX_PERF */
			num_ctids_per_page += chunk_cursor;

			MemSet(&ctid_chunk, 0, sizeof(CtidChunkData));
			chunk_cursor = 0;
		}

		cursor++;
	}

	if (chunk_cursor != 0)
	{
#ifdef VERSION_INDEX_PERF
		clock_gettime(CLOCK_MONOTONIC, &start);
#endif	/* VERSION_INDEX_PERF */
		/* Write out the left overs */
		*offset += pg_pwrite(fd, &ctid_chunk, sizeof(CtidData) * chunk_cursor,
							 *offset);
#ifdef VERSION_INDEX_PERF
		clock_gettime(CLOCK_MONOTONIC, &stop);
		vi_perf_ctx.pwrite_time +=
			(TIMESPEC_TO_MS(stop) - TIMESPEC_TO_MS(start));
#endif	/* VERSION_INDEX_PERF */
		num_ctids_per_page += chunk_cursor;
	}

	LWLockRelease(lock);

	return num_ctids_per_page;
}

/*
 * ViPageUpdateXminInteranl
 * 		Updates the xmin value of the corresponding page id and offset.
 */
void
ViUpdateXminInternal(ViBuffer frame_id,
					 ViPageOffset offset,
					 TransactionId xmin)
{
	ViPage page = (ViPage) ViBufferGetPage(frame_id);

	ViPageUpdateXmin(page, offset, xmin);
}

/*
 * ViPageUpdateXmaxInteranl
 * 		Updates the xmax value of the corresponding page id and offset.
 */
void
ViUpdateXmaxInternal(ViBuffer frame_id,
					 ViPageOffset offset,
					 TransactionId xmax)
{
	ViPage page = (ViPage) ViBufferGetPage(frame_id);

	ViPageUpdateXmax(page, offset, xmax);
}

void
ViUpdateSegmentOffsetInternal(ViBuffer frame_id,
							  ViPageOffset offset,
							  SegmentOffset new_seg_offset)
{
	ViPage page = (ViPage) ViBufferGetPage(frame_id);

	ViPageUpdateSegmentOffset(page, offset, new_seg_offset);
}

void
ViUpdateHintBitsInternal(ViBuffer frame_id,
						 ViPageOffset offset,
						 uint16 infomask)
{
	ViPage page = (ViPage) ViBufferGetPage(frame_id);

	ViPageUpdateHintBits(page, offset, infomask);
}

void
ViSetHintBitsInternal(ViBuffer frame_id,
					  ViPageOffset offset,
					  uint16 infomask)
{
	ViPage page = (ViPage) ViBufferGetPage(frame_id);

	ViPageSetHintBits(page, offset, infomask);
}


#ifdef VERSION_INDEX_DEBUG
static bool
ViCtidIsValid(Ctid ctid)
{
	if (!CtidOffsetIsValid(ctid->offset))
	{
		elog(LOG, "offset out of bound: %u", ctid->offset);
	}

	if (!CtidLengthIsValid(ctid->len))
	{
		elog(LOG, "len out of bound: %u", ctid->len);
	}

	if (!CtidHeaderLengthIsValid(ctid->h_len))
	{
		elog(LOG, "header len out of bound: %u", ctid->h_len);
	}

	if (!CtidBitsIsValid(ctid->bits))
	{
		elog(LOG, "null bits out of bound: %u", ctid->bits);
	}

	return (CtidOffsetIsValid(ctid->offset) &&
			CtidLengthIsValid(ctid->len) &&
			CtidHeaderLengthIsValid(ctid->h_len) &&
			CtidBitsIsValid(ctid->bits));
}
#endif	/* VERSION_INDEX_DEBUG */

#endif /* VERSION_INDEX */
