/*-------------------------------------------------------------------------
 *
 * vi_bufpage.c
 *	  Version index page internal operations.
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/storage/vi/vi_bufpage.c
 *
 *-------------------------------------------------------------------------
 */
#ifdef VERSION_INDEX

#include "postgres.h"

#include "port/atomics.h"
#include "storage/itemid.h"
#include "storage/buf.h"
#include "storage/bufpage.h"
#include "storage/lwlock.h"
#include "utils/rel.h"
#include "executor/executor.h"

#include "storage/vi.h"
#include "storage/vi_bufpage.h"
#include "storage/vi_buf.h"

#include <pthread.h>
#include <unistd.h>

#define ViPageSlotByteIsFull(byte) ((byte) == 0xFFU)

#define ViPageExtractOffsetBit(slots, offset) \
( \
	AssertMacro(offset < VI_PER_PAGE), \
	((slots)[(offset) >> 3] & (1 << ((offset) & 0x07))) \
)

#define ViPageMarkOffsetBit(slots, offset) \
( \
	AssertMacro(offset < VI_PER_PAGE), \
	AssertMacro(!((slots)[(offset) >> 3] & (1 << ((offset) & 0x07)))), \
	((slots)[(offset) >> 3] |= (1 << ((offset) & 0x07))) \
)

#define ViPageFreeOffsetBit(slots, offset) \
( \
	AssertMacro(offset < VI_PER_PAGE), \
	AssertMacro(((slots)[(offset) >> 3] & (1 << ((offset) & 0x07)))), \
	((slots)[(offset) >> 3] &= (~(1 << ((offset) & 0x07)))) \
)


/*
 * ViInitPerSegmentMeta
 * 		Initialize the meta page
 */
void
ViInitPerSegmentMeta(ViPerSegmentMeta meta)
{
	meta->last_slot = 0;
	meta->last_page_id = VI_META_PAGE_ID;
}

/*
 * ViPageIsFull
 * 		Checks the slots and returns if the page is full or not. At least a
 * 		shared lock is assumed to be held before entering this function.
 */
bool
ViPageIsFull(ViPage page)
{
	for (int i = 0; i < VI_NUM_SLOTS_IN_BYTE; i++)
	{
		if (!ViPageSlotByteIsFull(page->header.slots[i]))
			return false;
	}
	return true;
}

/*
 * ViPageAcquireFreeSlot
 * 		Acquires an empty slot within the page and returns it's offset. We must
 * 		ensure that a slot is available before entering this function. Also,
 * 		an exclusive lock must be held before entering this function.
 */
ViPageOffset
ViPageAcquireFreeSlot(ViPage page)
{
	for (ViPageOffset offset = 0; offset < VI_NUM_SLOTS; offset++)
	{
		if (!ViPageExtractOffsetBit(page->header.slots, offset))
		{
			ViPageMarkOffsetBit(page->header.slots, offset);
			return offset;
		}
	}

	/* Should never end up here */
	Assert(false);

	/* Dummy return for suppressing compiler warning */
	return VI_INVALID_PAGE_OFFSET;
}

void
ViMarkOffsetOccupied(ViBuffer frame_id,
					 ViPageOffset offset)
{
	ViPage page;

	page = (ViPage) ViBufferGetPage(frame_id);

	Assert(!ViPageExtractOffsetBit(page->header.slots, offset));
	ViPageMarkOffsetBit(page->header.slots, offset);
}

void
ViMarkOffsetFree(ViBuffer frame_id,
				 ViPageOffset offset)
{
	ViPage page;

	page = (ViPage) ViBufferGetPage(frame_id);

	Assert(ViPageExtractOffsetBit(page->header.slots, offset));
	ViPageFreeOffsetBit(page->header.slots, offset);
}

bool
ViPageOffsetIsOccupied(ViBuffer frame_id,
					   ViPageOffset offset)
{
	ViPage page;
	bool is_occupied;

	page = (ViPage) ViBufferGetPage(frame_id);

	is_occupied = ViPageExtractOffsetBit(page->header.slots, offset);

	return is_occupied;
}

void
ViPageUpdateXmin(ViPage page,
				 ViPageOffset offset,
				 TransactionId xmin)
{
	Assert(0 <= offset && offset < VI_NUM_SLOTS);
	Assert(ViPageExtractOffsetBit(page->header.slots, offset));
	page->vi_ctids[offset].xmin = xmin;
}

void
ViPageUpdateXmax(ViPage page,
				 ViPageOffset offset,
				 TransactionId xmax)
{
	Assert(0 <= offset && offset < VI_NUM_SLOTS);
	Assert(ViPageExtractOffsetBit(page->header.slots, offset));
	page->vi_ctids[offset].xmax = xmax;
}

void
ViPageUpdateSegmentOffset(ViPage page,
						  ViPageOffset offset,
						  SegmentOffset new_seg_offset)
{
	Assert(0 <= offset && offset < VI_NUM_SLOTS);
	Assert(ViPageExtractOffsetBit(page->header.slots, offset));
	page->vi_ctids[offset].ctid.offset = new_seg_offset;
}

void
ViPageUpdateHintBits(ViPage page,
					 ViPageOffset offset,
					 uint16 infomask)
{
	Assert(0 <= offset && offset < VI_NUM_SLOTS);
	Assert(ViPageExtractOffsetBit(page->header.slots, offset));
	page->vi_ctids[offset].infomask = infomask;
}

void
ViPageSetHintBits(ViPage page,
				  ViPageOffset offset,
				  uint16 infomask)
{
	Assert(0 <= offset && offset < VI_NUM_SLOTS);
	Assert(ViPageExtractOffsetBit(page->header.slots, offset));
	page->vi_ctids[offset].infomask |= infomask;
}

void
ViPageEnqueue(ViPerSegmentMeta meta,
			  ViPageId page_id)
{
	uint64 seq = __sync_fetch_and_add(&meta->rear, 1);
	int slot_idx = seq % QUEUE_SIZE;
	uint64 round = seq / QUEUE_SIZE;

	while (true)
	{
		uint64 flag = meta->queue[slot_idx].flag;
		if (flag % 2 == 1)
		{
			// queue full
			return;
		}
		else
		{
			if (flag / 2 == round)
			{
				// fairness
				meta->queue[slot_idx].free_page_id = page_id;
				__sync_synchronize();
				meta->queue[slot_idx].flag++;
				break;
			}
			else
			{
				pthread_yield();
			}
		}
	}
}

ViPageId
ViPageDequeue(ViPerSegmentMeta meta)
{
	uint64 seq = __sync_fetch_and_add(&meta->front, 1);
	int	slot_idx = seq % QUEUE_SIZE;
	uint64 round = seq / QUEUE_SIZE;
	ViPageId ret;

	while (true)
	{
		uint64 flag = meta->queue[slot_idx].flag;
		if (flag % 2 == 0)
		{
			// queue empty
			return VI_INVALID_PAGE_ID;
		}
		else
		{
			if (flag / 2 == round)
			{
				// fairness
				ret = meta->queue[slot_idx].free_page_id;
				__sync_synchronize();
				meta->queue[slot_idx].flag++;
				break;
			}
			else
			{
				pthread_yield();
			}
		}
	}

	return ret;
}

#ifdef VERSION_INDEX_DEBUG
TransactionId ViPageGetXmin(ViPage page,
							ViPageOffset offset)
{
	return page->vi_ctids[offset].xmin;
}

TransactionId ViPageGetXmax(ViPage page,
							ViPageOffset offset)
{
	return page->vi_ctids[offset].xmax;
}
#endif /* VERSION_INDEX_DEBUG */

#endif /* VERSION_INDEX */
