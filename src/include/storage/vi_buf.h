/*-------------------------------------------------------------------------
 *
 * vi_buf.h
 *	  Version index buffer manager data types.
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/vi_buf.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef VI_BUF_H
#define VI_BUF_H

#define InvalidViBuffer (0)

/*
 * ViBufferIsInvalid
 *		True iff the buffer is invalid.
 */
#define ViBufferIsInvalid(buffer) ((buffer) == InvalidViBuffer)

typedef struct ViBufferTag
{
	Oid		 rel_id;
	Oid		 seg_id;
	ViPageId page_id;
} ViBufferTag;

#define CLEAR_VI_BUFFERTAG(tag) \
( \
	(tag).rel_id = 0, \
	(tag).seg_id = 0, \
	(tag).page_id = InvalidBlockNumber \
)

#define INIT_VI_BUFFERTAG(tag, rel_id, seg_id, page_id) \
( \
	(tag).rel_id = rel_id, \
	(tag).seg_id = seg_id, \
	(tag).page_id = page_id \
)

#define VI_BUFFERTAGS_EQUAL(x, y) \
( \
	(x).rel_id == (y).rel_id && \
	(x).seg_id == (y).seg_id && \
	(x).page_id == (y).page_id \
)

#define ViBufTableHashPartition(hashcode) \
	((hashcode) % NUM_VI_BUFFER_PARTITIONS)
#define ViBufMappingPartitionLock(hashcode) \
	(&MainLWLockArray[VI_BUFFER_MAPPING_LWLOCK_OFFSET + \
					  ViBufTableHashPartition(hashcode)].lock)
#define ViBufMappingPartitionLockByIndex(i) \
	(&MainLWLockArray[VI_BUFFER_MAPPING_LWLOCK_OFFSET + (i)].lock)

extern PGDLLIMPORT char *ViBufferBlocks;

#define ViBufferGetPage(buffer) \
	((Page) (ViBufferBlocks + ((Size) (buffer)) * VI_PAGE_SIZE))

typedef struct ViBufferDesc
{
	/* ID of the cached block. block INVALID means this entry is unused */
	ViBufferTag tag;

	/* Whether the page is not yet synced */
	bool is_dirty;

	/*
	 * Buffer entries with refcnt > 0 should not be evicted.
	 * We use refcnt as a pin. The refcnt of an appending page should be
	 * kept 1 or higher, and the transaction which filled up the page
	 * should decrease it to unpin it.
	 */
	pg_atomic_uint32 refcnt;

	LWLock content_lock;
} ViBufferDesc;

/* Check BUFFERDESC_PAD_TO_SIZE */
#define VI_BUFFERDESC_PAD_TO_SIZE (SIZEOF_VOID_P == 8 ? 64 : 1)

typedef union ViBufferDescPadded {
	ViBufferDesc bufferdesc;
	char		 pad[VI_BUFFERDESC_PAD_TO_SIZE];
} ViBufferDescPadded;

extern PGDLLIMPORT ViBufferDescPadded *ViBufferDescriptors;

#define GetViBufferDescriptor(id) (&ViBufferDescriptors[(id)].bufferdesc)
#define GetViBufferIOLock() (&ViBufferIOLWLock->lock)

#define ViBufferDescriptorGetContentLock(bdesc) \
	((LWLock*) (&(bdesc)->content_lock))

extern PGDLLIMPORT LWLockMinimallyPadded *ViBufferIOLWLock;

typedef struct ViBufferMeta
{
	/*
	 * Indicate the cache entry which might be a victim for allocating
	 * a new page. Need to use fetch-and-add on this so that multiple
	 * transactions can allocate/evict cache entries concurrently.
	 */
	pg_atomic_uint64 eviction_rr_idx;
} ViBufferMeta;

#define VI_META_PAD_TO_SIZE (SIZEOF_VOID_P == 8 ? 64 : 1)

typedef struct ViBufferMetaPadded
{
	ViBufferMeta buffermeta;
	char		 pad[VI_META_PAD_TO_SIZE];
} ViBufferMetaPadded;

/* Shared memory stuff for version index buffer pool. */
extern void InitViBufferPool(void);
extern Size ViBufferShmemSize(void);

/* Version index buffer operations. */
extern ViBuffer ViGetPage(Oid rel_id, Oid seg_id, ViPageId page_id,
						  bool is_new);
extern int 	ViOpenFile(Oid rel_id, Oid seg_id, int flags);
extern void ViReleasePage(ViBuffer frame_id);
extern void	ViMarkPageDirty(ViBuffer frame_id);
extern void ViWritePageExternal(const ViBufferTag *tag, ViBuffer frame_id);

#endif /* VI_BUF_H */
