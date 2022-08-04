#ifndef DIRTY_BUFID_H_
#define DIRTY_BUFID_H_

#ifdef SMARTSSD_DBUF

#include "postgres.h"

#include <stdint.h>

#include "utils/relcache.h"
#include "storage/lwlock.h"
#include "storage/buf_internals.h"

#include "storage/proc.h"
#include "storage/shmem.h"
#include "storage/ipc.h"
#include "utils/dsa.h"
#include "utils/hsearch.h"

#include "storage/ndp_support.h"
#define NUM_BUFIDSET_PARTITION 128
#define CYCLE_INIT 0
#define INITNUMREL 32
#define INITNUMSEG 32
#define SOFTMAXNUMREL 128
#define NUMSEG_INIT 1

typedef struct BufIDNodeData
{
	volatile int buf_id;
	volatile BlockNumber blk_num;
	volatile int next_node_idx;
	volatile int prev_node_idx;
#ifdef SMARTSSD_DBUF_DEBUG
	volatile uint32 relid;
	volatile uint32 segno;
#endif
} BufIDNodeData;

typedef struct BufIDNodeData* BufIDNode;

#define INVALIDIDX (-1)
#ifndef SMARTSSD_DBUF_DEBUG
#define FREEDBUFNODEPADSIZE 3
#else
#define FREEDBUFNODEPADSIZE 5
#endif

typedef struct FreeDBufNodeData
{
	volatile int prev_idx;
	volatile int pad[FREEDBUFNODEPADSIZE];
} FreeDBufNodeData;

typedef struct FreeDBufNodeData * FreeDBufNode;

typedef struct DirtyBufIDListData
{
	volatile int num_elem;
	volatile uint32 flush_times;
	volatile int head_node_idx;
	volatile int tail_node_idx;
	volatile int global_lock_number;
} DirtyBufIDListData;

typedef struct DirtyBufIDListData* DirtyBufIDList;

typedef struct DBufTag
{
	Oid relid;
	Oid segno;
} DBufTag;

typedef struct DBufLookupEnt
{
	volatile DBufTag key;
	DirtyBufIDListData dbufidlist;
} DBufLookupEnt;

typedef struct TTTag
{
	Oid relid;
} TTTag;

typedef struct TTLookupEnt
{
	TTTag key;
	bool is_target;
} TTLookupEnt;

typedef struct DirtyBufIDPtrsData
{
	Oid rel_id;
	int num_seg;
	int max_num_seg;

	volatile dsa_pointer dirty_buf_id_ptrs;

	LWLock seg_alloc_lock;
} DirtyBufIDPtrsData;

typedef struct DirtyBufIDPtrsData* DirtyBufIDPtrs;

typedef struct FreeNodeStackData {
	volatile int stack_head_idx;
#ifdef SMARTSSD_DBUF_DEBUG
	volatile int num_elem;
#endif
} FreeNodeStackData;

typedef struct FreeNodeStackData * FreeNodeStack;

typedef struct BufIDSetData
{
	volatile int alloced_number;
	FreeNodeStackData free_node_stack;
} BufIDSetData;

typedef struct BufIDSetData* BufIDSet;

typedef struct FlushDirtyBufIDListData
{
	Oid relid;
	uint32 segno;
	int num_elem;
	uint32 flush_times; 
	int * dirty_buf_ids_arr;
	BlockNumber * blk_num_arr;
} FlushDirtyBufIDListData;

typedef struct FlushDirtyBufIDListData* FlushDirtyBufIDList;

Size NDPBufIDSetShmemSize(void);
void NDPBufIDSetInit(void);
void NDPInitFreeNodeStack(void);
void NDPAttachDsaDirtyBufIDs(void);
void NDPDetachDsaDirtyBufIDs(void);

int NDPFindDBufIDList(Oid rel_id, bool no_trx);

dsa_pointer NDPGetDirtyBufListSegPtr(int idx);
dsa_pointer NDPGetDirtyBufIDListPtr(dsa_pointer dbuf_list_segs_ptr, 
									uint32 seg_no);
dsa_pointer NDPInitDirtyBufIDSet(void);
dsa_pointer NDPGetDPtrDBufIDsList(Oid relid, BlockNumber blkno,
								  bool need_to_alloc);
#ifdef SMARTSSD_DBUF_DEBUG
void NDPSetDirtyBufID(volatile DirtyBufIDList dbuflist, Buffer buf, Oid relid,
											Oid segno);
#else
void NDPSetDirtyBufID(volatile DirtyBufIDList dbuflist, Buffer buf);
#endif
#ifdef SMARTSSD_DBUF_DEBUG
void NDPPopDirtyBufID(volatile DirtyBufIDList dbuflist, Buffer buf, Oid relid,
											Oid segno);
#else
void NDPPopDirtyBufID(volatile DirtyBufIDList dbuflist, Buffer buf);
#endif
FlushDirtyBufIDList NDPDetachDirtyBufIDs(volatile DirtyBufIDList dbuflist, 
										 Oid relid, uint32 segno,
										 LWLock * list_content_lock);
bool IsTargetBuffer(Oid relid);
uint32 NDPDBufHashCode(const void * keydbuf, HTAB * hash_table_ptr);
DBufLookupEnt * NDPHashLookup(const void * keydbuf, uint32 hashcode,
							 HTAB * hash_table_ptr);
DBufLookupEnt * NDPHashInsert(DBufTag * keydbuf, uint32 hashcode, 
							  HTAB * hash_table_ptr);
bool NDPHashDelete(const void * keydbuf, uint32 hashcode, 
				   HTAB * hash_table_ptr);
void NDPPushFreeNode(FreeDBufNode push_node);
void IsPointingRightNode2(int test_node_idx, int test_buf_id);

#define DBufHeapIDLock() (&MainLWLockArray[DBUF_HEAPID_HASH_LWLOCK_OFFSET].lock)

#define DBufHashPartition(hashcode) ((hashcode) % NUM_DBUF_CHAIN_ALLOC)

#define DBufChainAllocLock(hashcode) \
( \
	&MainLWLockArray[DBUF_CHAIN_ALLOC_LWLOCK_OFFSET + \
					 DBufHashPartition(hashcode)].lock \
)

#define DBufFStackLock() (&MainLWLockArray[DBUF_FSTACK_LWLOCK_OFFSET].lock)
#define DBufDListLock(idx) \
		(&MainLWLockArray[DBUF_LIST_LWLOCK_OFFSET + idx].lock)

#endif //#ifdef SMARTSSD_DBUF

#endif
