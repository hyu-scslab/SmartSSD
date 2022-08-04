/*-------------------------------------------------------------------------
 *
 * vi.h
 *	  Version index data type declarations.
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/vi.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef VI_H
#define VI_H

typedef uint32 ViPageId;
typedef uint32 ViPageOffset;

typedef int ViBuffer;

typedef uint32 SegmentOffset;

#define MAX_SEGMENT_OFFSET ((SegmentOffset)0x40000000)

/* Absolute offset among 1GiB */
#define GetSegmentOffset(blockno, pos) \
	(((blockno * BLCKSZ) % MAX_SEGMENT_OFFSET) + pos)

typedef struct __attribute__((__packed__)) CtidData
{
	unsigned offset : 30,
			 len 	: 10,
			 h_len	: 9,
			 bits	: 23;
} CtidData;

typedef CtidData* Ctid;

#define CtidOffsetIsValid(offset) (offset <= 0x3FFFFFFFU)
#define CtidLengthIsValid(len) (len <= 0x3FFU)
#define CtidHeaderLengthIsValid(h_len) (h_len <= 0x1FFU)
#define CtidBitsIsValid(bits) (bits <= 0x7FFFFF)

// #define SmartSSDChunkSize (64) TODO: uncomment if 64 byte chunk is needed.
#define SmartSSDChunkSize (63)
#define CtidsPerChunk (SmartSSDChunkSize / sizeof(CtidData))

typedef struct __attribute__((__packed__)) CtidChunkData
{
	CtidData ctids[CtidsPerChunk];
	// char pad[1]; TODO: uncomment if 64 byte chunk is needed.
} CtidChunkData;

typedef CtidChunkData* CtidChunk;

/*
 * Version index structure.
 * CTID = Raw CTID + (xmin, xmax)
 *
 * +-------------------------------+
 * | xmin, xmax (64 bit)           |
 * +-------------------------------+--\
 * | in-segment offset (30 bit)    |   \
 * +-------------------------------+    \
 * | version length (10 bit)       |    |
 * +-------------------------------+   CTID (Canonical Tuple IDentifier)
 * | version header length (8 bit) |    |
 * +-------------------------------+    /
 * | NULL bitmap (23 bit)          |   /
 * +-------------------------------+--/
 */
typedef struct ViCtidData
{
	TransactionId xmin;
	TransactionId xmax;
	uint16 infomask;

	CtidData ctid;
} ViCtidData;

typedef ViCtidData* ViCtid;

extern void ViExtractAndBuildViCtid(BlockNumber blkno, Page page_header,
									OffsetNumber offnum, ViCtid dest);
extern int ViPutViCtid(Relation relation, BlockNumber blkno, Page page_header,
					   OffsetNumber offnum,ViCtid vi_ctid);
extern int ViPrescreen(Oid rel_id, Oid seg_id, Snapshot snapshot, bool reuse);
extern void ViUpdateXmin(Oid rel_id, Oid seg_id, ViPageId page_id,
						 ViPageOffset offset, TransactionId xmin);
extern void ViUpdateXmax(Oid rel_id, Oid seg_id, ViPageId page_id,
						 ViPageOffset offset, TransactionId xmax);
extern void ViUpdateSegmentOffset(Oid rel_id, Oid seg_id, ViPageId page_id,
								  ViPageOffset offset,
								  SegmentOffset new_seg_offset);
extern void ViUpdateHintBits(Oid rel_id, Oid seg_id, ViPageId page_id,
							 ViPageOffset offset, uint16 infomask);
extern void ViSetHintBits(Oid rel_id, Oid seg_id, ViPageId page_id,
						  ViPageOffset offset, uint16 infomask);
extern bool ViCheckVisibility(TransactionId xmin, TransactionId xmax,
							  uint16 infomask, Snapshot snapshot);
/* required by vacuum process */
extern void ViFreeOffset(Oid rel_id, Oid seg_id, ViPageId page_id,
						 ViPageOffset offset);

#ifdef VERSION_INDEX_DEBUG
/* For debugging */
extern TransactionId ViGetXmin(Oid rel_id, Oid seg_id, ViPageId page_id,
							   ViPageOffset offset);
extern TransactionId ViGetXmax(Oid rel_id, Oid seg_id, ViPageId page_id,
							   ViPageOffset offset);
extern bool ViDebugCtidEquals(char* CTID,
							  TransactionId xmin, TransactionId xmax,
							  Oid rel_id, Oid seg_id,
							  ViPageId page_id, ViPageOffset offset);
#endif /* VERSION_INDEX_DEBUG */

#ifdef VERSION_INDEX_PERF
typedef struct ViPerfContext
{
	struct timespec start, stop;
	uint64 visibility_check; 
	uint64 memcpy_time; 
	uint64 pwrite_time; 
} ViPerfContext;

/* in vimanager.c */
extern ViPerfContext vi_perf_ctx;
extern char vi_perf_fname[32];

#define VI_INIT_PERF_CONTEXT(ctx) \
( \
	ctx.visibility_check = 0, \
	ctx.memcpy_time = 0, \
	ctx.pwrite_time = 0 \
)
#define TIMESPEC_TO_MS(ts) \
( \
  	(uint64) (ts.tv_sec * 1000 + ts.tv_nsec / 1000000) \
)
#endif /* VERSION_INDEX_PERF */

#endif /* VI_H */
