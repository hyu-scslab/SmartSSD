/*-------------------------------------------------------------------------
 *
 * vi_bufpage.h
 *	  Internal operations in version index buffer page
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/vi_bufpage.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef VI_BUFPAGE_H
#define VI_BUFPAGE_H

/*
 * Each version index page is comprised of 8192 bytes.
 */
#define VI_PAGE_SIZE ((Size)(1U << 13))		  /* 8192 */

#define VI_INVALID_PAGE_ID ((ViPageId) 0xFFFFFFFFU)
#define VI_MAX_PAGE_ID ((ViPageId) 0xFFFFFFFEU)
#define VI_META_PAGE_ID ((ViPageId) 0x0U)

#define VI_INVALID_PAGE_OFFSET ((ViPageOffset) 0xFFFFFFFFU)

#define ViPageIdIsValid(page_id) \
	((bool) ((ViPageId) (page_id) != VI_INVALID_PAGE_ID))
#define ViPageOffsetIsValid(offset) \
	((bool) ((ViPageOffset) (offset) != VI_INVALID_PAGE_OFFSET))

typedef struct __attribute__((__packed__)) ViQueueData
{
	ViPageId free_page_id;
	uint64 flag;
} ViQueueData;

/* 
 * Meta page structure (page #0)
 */
#define QUEUE_SIZE (680)

typedef struct ViPerSegmentMetaData
{
	ViPageOffset last_slot; /* last witnessed unused slot index */
	ViPageId last_page_id;	/* last page id of the current vi file */
	TransactionId min_xmin; /* minimum ximin of the entire page */

	/* concurrent static queue for free page list management */
	uint64 front;
	uint64 rear;
	ViQueueData queue[680]; /* free page id queue */
} ViPerSegmentMetaData;

typedef ViPerSegmentMetaData *ViPerSegmentMeta;

extern void ViInitPerSegmentMeta(ViPerSegmentMeta meta);

extern void ViPageEnqueue(ViPerSegmentMeta meta, ViPageId page_id);
extern ViPageId ViPageDequeue(ViPerSegmentMeta meta);



/* 
 * Actual version index page (page #1 ~ VI_MAX_PAGE_ID)
 */

/*
 *              ViPage (8192 byte)
 * +------------------------------------+--
 * |        slot array (50 byte)        |  \
 * +------------------------------------+   + -> ViPageHeaderData
 * |         padding   (142 byte)       |  /
 * +------------------------------------+--
 * |                                    |  \
 * |     ViCtidData[400] (8000byte)     |   + -> Actual data (ViCtids)
 * |                                    |  /
 * +------------------------------------+--
 *
 */

#define VI_NUM_SLOTS (400)
#define VI_NUM_SLOTS_IN_BYTE (VI_NUM_SLOTS / 8)
#define VI_PAGE_HEADER_SIZE (192)

typedef struct ViPageHeaderData
{
	/* 50 byte slot array + padding = 192 byte */
	bits8 slots[VI_NUM_SLOTS_IN_BYTE];
	char pad[VI_PAGE_HEADER_SIZE - VI_NUM_SLOTS_IN_BYTE];
} ViPageHeaderData;

#define VI_PER_PAGE ((VI_PAGE_SIZE - VI_PAGE_HEADER_SIZE) / sizeof(ViCtidData))

typedef struct ViPageData
{
	ViPageHeaderData header;
	ViCtidData vi_ctids[VI_PER_PAGE];
} ViPageData;

typedef ViPageData* ViPage;

extern bool ViPageIsFull(ViPage page);
extern ViPageOffset ViPageAcquireFreeSlot(ViPage page);
extern void ViMarkOffsetOccupied(ViBuffer frame_id, ViPageOffset offset);
extern void ViMarkOffsetFree(ViBuffer frame_id, ViPageOffset offset);

extern bool ViPageOffsetIsOccupied(ViBuffer frame_id, ViPageOffset offset);

extern void ViPageUpdateXmin(ViPage page, ViPageOffset offset,
							 TransactionId xmin);
extern void ViPageUpdateXmax(ViPage page, ViPageOffset offset,
							 TransactionId xmax);
extern void ViPageUpdateSegmentOffset(ViPage page, ViPageOffset offset,
									  SegmentOffset new_seg_offset);
extern void ViPageUpdateHintBits(ViPage page, ViPageOffset offset,
								 uint16 infomask);
extern void ViPageSetHintBits(ViPage page, ViPageOffset offset,
							  uint16 infomask);

#ifdef VERSION_INDEX_DEBUG
extern TransactionId ViPageGetXmin(ViPage page, ViPageOffset offset);
extern TransactionId ViPageGetXmax(ViPage page, ViPageOffset offset);
#endif /* VERSION_INDEX_DEBUG*/

#endif /* VI_BUFPAGE_H */
