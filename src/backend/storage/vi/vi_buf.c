/*-------------------------------------------------------------------------
 *
 * vi_buf.c
 *	  Version index buffer manager data types.
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/storage/vi/vi_buf.c
 *
 *-------------------------------------------------------------------------
 */
#ifdef VERSION_INDEX

#include "postgres.h"

#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/block.h"
#include "storage/itemid.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "storage/bufmgr.h"
#include "storage/buf.h"
#include "storage/bufpage.h"
#include "access/htup.h"

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
#include "storage/vi_hash.h"

#include "postmaster/vimanager.h"

/* in globals.c ... this duplicates miscadmin.h */
extern PGDLLIMPORT int NViBuffers;

/* Shared memory objects */
ViBufferDescPadded *   ViBufferDescriptors;
char *				   ViBufferBlocks;
ViBufferMetaPadded *   ViBufferMetadata;
LWLockMinimallyPadded *ViBufferIOLWLock = NULL;

const char *vi_fname_suffix = "vi";

/*
 * TODO: change to a sufficient value when page allocation logic is implemented
 */
#define VI_INIT_FILE_SIZE (VI_PAGE_SIZE * 0)

static void ViInitMeta(void);

static ViBuffer ViGetPageInternal(Oid rel_id,
								  Oid seg_id,
								  ViPageId page_id,
								  bool is_new);

static bool ViMetaPageExists(ViBufferTag *tag);
static void ViInitFile(Oid rel_id, Oid seg_id);
static void ViCloseFile(int fd);
static void ViReadPage(const ViBufferTag *tag, ViBuffer frame_id);
static void ViWritePage(const ViBufferTag *tag, ViBuffer frame_id);
static void ViReleasePageInternal(ViBufferDesc *frame);

/*
 * ViBufferShmemSize
 *		Compute the size of shared memory for the buffer pool including
 *		data pages, buffer descriptors, hash tables, etc.
 */
Size
ViBufferShmemSize(void)
{
	Size size = 0;

	/* Size of version index buffer descriptors */
	size = add_size(size, mul_size(NViBuffers, sizeof(ViBufferDescPadded)));

	/* To allow aligning buffer descriptors */
	size = add_size(size, PG_CACHE_LINE_SIZE);

	/* Data pages */
	size = add_size(size, mul_size(NViBuffers, VI_PAGE_SIZE));

	/* LWLocks */
	size = add_size(size, mul_size(NViBuffers, sizeof(LWLockMinimallyPadded)));

	/* To allow aligning lwlocks */
	size = add_size(size, PG_CACHE_LINE_SIZE);

	/* Version index buffer hash */
	size =
		add_size(size, ViHashShmemSize(NViBuffers + NUM_VI_BUFFER_PARTITIONS));

	/* To allow aligning lwlocks */
	size = add_size(size, PG_CACHE_LINE_SIZE);

	/* Version index buffer IO lock */
	size = add_size(size, sizeof(LWLockMinimallyPadded));

	/* Version index buffer metadata */
	size = add_size(size, sizeof(ViBufferMetaPadded));

	return size;
}

/*
 * InitViBufferPool
 * 		Initialize shared buffer pool.  This is called once during shared-memory
 * 		initialization (either in the postmaster, or in a standalone backend).
 */
void
InitViBufferPool(void)
{
	bool foundDescs, foundBufs, foundMeta, foundIOLocks;
	ViBufferDesc *buf;
	int	i;

	/* Align descriptors to a cacheline boundary */
	ViBufferDescriptors = (ViBufferDescPadded *) ShmemInitStruct(
		"Version Index Buffer Descriptors",
		NViBuffers * sizeof(ViBufferDescPadded), &foundDescs);

	/* Buffer blocks */
	ViBufferBlocks = (char *) ShmemInitStruct(
		"Version Index Buffer Blocks", NViBuffers * ((Size)(VI_PAGE_SIZE)),
		&foundBufs);

	/* Allocate and initialize hashtable */
	ViHashInit(NViBuffers + NUM_VI_BUFFER_PARTITIONS);

	LWLockRegisterTranche(LWTRANCHE_VI_BUFFER_CONTENT, "vi_buffer_content");

	/* Initialize descriptors */
	for (i = 0; i < NViBuffers; i++)
	{
		buf = GetViBufferDescriptor(i);
		CLEAR_VI_BUFFERTAG(buf->tag);
		buf->is_dirty = false;
		pg_atomic_init_u32(&buf->refcnt, 0);

		LWLockInitialize(ViBufferDescriptorGetContentLock(buf),
						 LWTRANCHE_VI_BUFFER_CONTENT);
	}

	/* Allocate metadata */
	ViBufferMetadata = (ViBufferMetaPadded *) ShmemInitStruct(
		"Version Index Buffer Metadata", sizeof(ViBufferMetaPadded),
		&foundMeta);

	ViBufferIOLWLock = (LWLockMinimallyPadded *) ShmemInitStruct(
		"Version Index Buffer IO Lock", sizeof(LWLockMinimallyPadded),
		&foundIOLocks);

	LWLockInitialize(GetViBufferIOLock(), LWTRANCHE_VI_BUFFER_IO);

	ViInitMeta();
}

/*
 * ViInitMeta
 * 		Initialize the meta data for ViBuffer.
 */
static void
ViInitMeta(void)
{
	ViBufferMeta *meta;

	meta = &ViBufferMetadata->buffermeta;

	pg_atomic_init_u64(&meta->eviction_rr_idx, 0);
}

/*
 * ViGetPage
 * 		Fetches and holds a pin to a buffer containing the requested page.
 * 		The page is read from disk if not loaded already. You must call
 * 		ViReleasePage() to release the reference count used for pinning.
 */
ViBuffer
ViGetPage(Oid rel_id,
		  Oid seg_id,
		  ViPageId page_id,
		  bool is_new)
{
	ViBuffer frame_id;

	frame_id = ViGetPageInternal(rel_id, seg_id, page_id, is_new);
	Assert(frame_id < NViBuffers);

	return frame_id;
}

/*
 * ViGetPageInternal
 * 		The actual internal operations necessary for requesting, loading and
 * 		pinning a page into the buffer.
 */
static ViBuffer
ViGetPageInternal(Oid rel_id,
				  Oid seg_id,
				  ViPageId page_id,
				  bool is_new)
{
	ViBufferTag vi_tag, victim_tag;
	int frame_id, candidate_id;
	LWLock *new_partition_lock;
	LWLock *old_partition_lock;
	uint32 hashcode, hashcode_vict;
	ViBufferDesc *frame;
	int ret;

	/* Set page id for hashing */
	CLEAR_VI_BUFFERTAG(vi_tag);
	INIT_VI_BUFFERTAG(vi_tag, rel_id, seg_id, page_id);

	/* Get hashcode based on page id */
	hashcode		   = ViHashCode(&vi_tag);
	new_partition_lock = ViHashMappingPartitionLock(hashcode);

	LWLockAcquire(new_partition_lock, LW_SHARED);
	frame_id = ViHashLookup(&vi_tag, hashcode);

	if (frame_id >= 0)
	{
		/* Target page is already in the buffer */
		frame = GetViBufferDescriptor(frame_id);

		/* Increase refcnt by 1, so this page can't be evicted */
		pg_atomic_fetch_add_u32(&frame->refcnt, 1);
		LWLockRelease(new_partition_lock);

		return frame_id;
	}

	LWLockRelease(new_partition_lock);

find_cand:
	/* Pick up a candidate entry for a new allocation */
	candidate_id = 
		pg_atomic_fetch_add_u64(
			&ViBufferMetadata->buffermeta.eviction_rr_idx, 1) % NViBuffers;

	frame = GetViBufferDescriptor(candidate_id);

	if (pg_atomic_read_u32(&frame->refcnt) != 0)
	{
		/* Someone is accessing this entry simultaneously, find another one */
		goto find_cand;
	}

	victim_tag = frame->tag;

	if (victim_tag.rel_id > 0)
	{
		/*
		 * This entry is in use now, so we need to remove the cache entry for
		 * it. We also need to flush it if the page is dirty.
		 */
		hashcode_vict = ViHashCode(&victim_tag);
		old_partition_lock = ViHashMappingPartitionLock(hashcode_vict);

		if (LWLockHeldByMe(old_partition_lock))
		{
			/* Partition lock collision occured by myself */
			goto find_cand;
		}

		if (!LWLockConditionalAcquire(old_partition_lock, LW_EXCLUSIVE))
		{
			/* Partition lock is already held by someone else */
			goto find_cand;
		}

		/* Try to hold refcnt for the eviction */
		ret = pg_atomic_fetch_add_u32(&frame->refcnt, 1);

		if (ret > 0)
		{
			/*
			 * Race occurred. Another read transaction might get this page,
			 * or possibly another eviciting transaction might get this page
			 * if round robin cycle is too short.
			 */
			pg_atomic_fetch_sub_u32(&frame->refcnt, 1);

			LWLockRelease(old_partition_lock);

			goto find_cand;
		}

		if (!VI_BUFFERTAGS_EQUAL(frame->tag, victim_tag))
		{
			/*
			 * This exception might be very rare, but the possible scenario is,
			 * 1. txn A processed up to just before holding the
			 * 	  old_partition_lock
			 * 2. Round robin cycle is too short, so txn B acquired the
			 *    old_partition_lock, and evicted this page, and mapped it
			 *    to another cache hash entry
			 * 3. Txn B unreffed this page after using it so that the refcnt
			 *    becomes 0, but rel_id of this entry have changed
			 * In this case, just find another victim for simplicity now.
			 */
			pg_atomic_fetch_sub_u32(&frame->refcnt, 1);

			LWLockRelease(old_partition_lock);

			goto find_cand;
		}

		Assert(VI_BUFFERTAGS_EQUAL(frame->tag, victim_tag));

		/*
		 * Now we are ready to use this entry as a new cache.
		 * First, check whether this victim should be flushed.
		 * Appending page shouldn't be picked as a victim because of the refcnt.
		 */
		if (frame->is_dirty)
		{
			ViWritePage(&frame->tag, candidate_id);
			frame->is_dirty = false;
		}

		/*
		 * Now we can safely evict this entry.
		 * Remove corresponding hash entry for it so that we can release the
		 * partition lock.
		 */
		ViHashDelete(&frame->tag, hashcode_vict);
		LWLockRelease(old_partition_lock);
	}
	else
	{
		/*
		 * This entry is unused. Increase refcnt and use it.
		 */
		ret = pg_atomic_fetch_add_u32(&frame->refcnt, 1);
		if (ret > 0)
		{
			/*
			 * Race occurred. Possibly another evicting transaction might get
			 * this page if round robin cycle is too short.
			 */
			pg_atomic_fetch_sub_u32(&frame->refcnt, 1);
			goto find_cand;
		}
		else if (frame->tag.rel_id != 0)
		{
			pg_atomic_fetch_sub_u32(&frame->refcnt, 1);
			goto find_cand;
		}
		Assert(!frame->is_dirty);
	}

	LWLockAcquire(new_partition_lock, LW_EXCLUSIVE);
	frame_id = ViHashLookup(&vi_tag, hashcode);

	if (frame_id >= 0)
	{
		Assert(frame_id != candidate_id);

		frame->tag.rel_id = 0;

		pg_memory_barrier();
		frame->tag.seg_id = 0;
		/* TODO: check if it shouldn't be ViInvalidPageId*/
		frame->tag.page_id = 0;

		pg_atomic_fetch_sub_u32(&frame->refcnt, 1);

		frame = GetViBufferDescriptor(frame_id);

		pg_atomic_fetch_add_u32(&frame->refcnt, 1);
		Assert(VI_BUFFERTAGS_EQUAL(frame->tag, vi_tag));

		LWLockRelease(new_partition_lock);
		return frame_id;
	}

	frame->tag = vi_tag;

	/*
	 * If requested page is not in the disk yet, we don't need to read it
	 * from the disk, and it's actually not in the disk.
	 */
	ret = ViHashInsert(&vi_tag, hashcode, candidate_id);
	Assert(ret == -1);

	/* 
	 * NOTE: Currently, the meta page initialization is done here. It would be
	 * better if the meta page initialization is done when each segment is
	 * created, considering concurrent insert/update situations.
	 */
	if (frame->tag.page_id == VI_META_PAGE_ID)
	{
		/* This read case occurs only when meta page was evicted previously */
		bool meta_exists = ViMetaPageExists(&frame->tag);

		if (!meta_exists)
		{
			/* 0 alloc all contents of ViPage */
			ViMarkPageDirty(candidate_id);
			ViInitFile(frame->tag.rel_id, frame->tag.seg_id);
			is_new = true;
		}
	}

	if (!is_new)
	{
		ViReadPage(&frame->tag, candidate_id);
	}
	else
	{
		MemSet(ViBufferGetPage(candidate_id), 0, VI_PAGE_SIZE);
	}

	LWLockRelease(new_partition_lock);

	/* Return the index of cache entry, holding refcnt 1 */
	return candidate_id;
}

/*
 * ViMetaPageExists
 * 		Check if the version index file exists.
 *
 * 	NOTE: Opening a file is an overhead. This should be optimized later on.
 */
static bool
ViMetaPageExists(ViBufferTag *tag)
{
	int fd = ViOpenFile(tag->rel_id, tag->seg_id, O_RDWR);

	if (fd < 0)
	{
		return false;
	}
	ViCloseFile(fd);

	return true;
}

/*
 * ViInitFile
 * 		Create a version index file.
 */
static void
ViInitFile(Oid rel_id,
		   Oid seg_id)
{
	int fd;

	fd = ViOpenFile(rel_id, seg_id, O_RDWR | O_CREAT);
	Assert(fd >= 0);

	close(fd);
}

/*
 * ViOpenFile
 * 		Opens a file corresponding to the relation and segment ID.
 * 		(e.g., 16372.2.i)
 */
int 
ViOpenFile(Oid rel_id,
		   Oid seg_id,
		   int flags)
{
	int  fd;
	char filename[64];

	sprintf(filename, "%u.%u.%s", rel_id, seg_id, vi_fname_suffix);
	fd = open(filename, flags, (mode_t)0600);

	return fd;
}

/*
 * ViCloseFile
 * 		Close the file by destroying the file descriptor.
 */
static void
ViCloseFile(int fd)
{
	Assert(fd >= 0);
	close(fd);
}

/*
 * ViReadPage
 * 		Reads a page to the buffer from a version index file.
 */
static void
ViReadPage(const ViBufferTag *tag,
		   ViBuffer frame_id)
{
	ssize_t read_size;

	int fd = ViOpenFile(tag->rel_id, tag->seg_id, O_RDWR);
	Assert(fd >= 0);

	read_size = pg_pread(fd, &ViBufferBlocks[frame_id * VI_PAGE_SIZE],
						 VI_PAGE_SIZE, tag->page_id * VI_PAGE_SIZE);
	Assert(read_size == VI_PAGE_SIZE);

	ViCloseFile(fd);
}

/*
 * ViWritePage
 * 		Write a page from the buffer to the version index file.
 *
 * 	NOTE: the current version does not require an IO lock since we do not have
 * 	another writer else than the version index writer. However, if a compaction
 * 	logic is added, we might have to consider using an IO lock. (TODO)
 */
static void
ViWritePage(const ViBufferTag *tag,
			ViBuffer frame_id)
{
	ssize_t write_size;

	// LWLockAcquire(&ViBufferIOLWLock->lock, LW_SHARED);

	int fd = ViOpenFile(tag->rel_id, tag->seg_id, O_RDWR);
	Assert(fd >= 0);

	write_size = pg_pwrite(fd, &ViBufferBlocks[frame_id * VI_PAGE_SIZE],
						   VI_PAGE_SIZE, tag->page_id * VI_PAGE_SIZE);
	Assert(write_size == VI_PAGE_SIZE);

	ViCloseFile(fd);

	// LWLockRelease(&ViBufferIOLWLock->lock);
}

/*
 * ViWritePageExternal
 * 		External API for flushing version index buffer.
 */
void
ViWritePageExternal(const ViBufferTag *tag,
					ViBuffer frame_id)
{
	ViWritePage(tag, frame_id);
}

/*
 * ViReleasePage
 * 		Release the pinned page by decrementing it's reference count by 1.
 */
void
ViReleasePage(ViBuffer frame_id)
{
	ViBufferDesc *frame;

	Assert(frame_id < NViBuffers);

	frame = GetViBufferDescriptor(frame_id);
	ViReleasePageInternal(frame);
}

/*
 * ViReleasePageInternal
 * 		Internal operation of ViReleasePage
 */
static void
ViReleasePageInternal(ViBufferDesc *frame)
{
	Assert(pg_atomic_read_u32(&frame->refcnt) != 0);
	pg_atomic_fetch_sub_u32(&frame->refcnt, 1);
}

/*
 * ViMarkPageDirty
 * 		Mark the page dirty by modifying the descriptor's flag.
 */
void
ViMarkPageDirty(ViBuffer frame_id)
{
	ViBufferDesc *desc;

	desc = GetViBufferDescriptor(frame_id);
	desc->is_dirty = true;
}

#endif /* VERSION_INDEX */
