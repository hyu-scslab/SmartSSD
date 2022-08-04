/*---------------------------------------------------------------------
 *
 *
 * dirty_bufid.c
 *		routines to managea dirty buf id sets for page flush per relation.
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *		src/backend/storage/ssd/dirty_bufid.c
 *---------------------------------------------------------------------
 */

#ifdef SMARTSSD_DBUF

#include "storage/ndp_dirty_bufid.h"

#include <stdlib.h>
#include <unistd.h>

#include "access/xact.h"
#include "catalog/catalog.h"

#include "storage/bufmgr.h"
#include "storage/ipc.h"
#include "storage/lmgr.h"
#include "storage/ndp_support_hash.h"
#include "storage/proc.h"
#include "storage/shmem.h"

#include "utils/dsa.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/resowner_private.h"

#include <stdlib.h>

#define FSTACKSIZE (NBuffers * 4)

extern PROC_HDR *ProcGlobal; /* Global proc pointer shared within one db 
								cluster used to initialize module components */
extern Oid		MyDatabaseId;
extern bool		IsUnderPostmaster;
extern PGPROC	*MyProc;

static BufIDNode NDPPopFreeNode(void);
void NDPPushFreeNode(FreeDBufNode push_node);
static Size NDPDBufHashShmemSize(Size size, Size ent_size);
static void
NDPDBufHashInit(Size init_size, Size max_size, HTAB ** hash_table_dptr, 
				Size tag_size, Size ent_size, int part_num, const char* hname,
				bool found);

#ifdef SMARTSSD_DBUF_DEBUG
static bool CheckIsInFreeNode(int checked_idx);
#endif

static bool IsPointingRightNode(int test_node_idx, int test_buf_id);
static MemoryContext MdCxt_dbuflist = NULL;

/* Pointer to go into dirty buffer IDs lists */
static BufIDSet buf_id_set_desc = NULL;
BufIDNode ndp_stack_base = NULL;

#ifdef SMARTSSD_DBUF_DEBUG
static int GetNodeNumInDList(int head_node_idx, int prev_node_idx, 
							 bool verbose);
#endif

HTAB * SharedBufIDSetHash = NULL;
HTAB * TargetTableIDHash = NULL;

static Size
NDPDBufHashShmemSize(Size size, Size ent_size)
{
	return HashGetShmemSize(size, ent_size);
}

static void
NDPDBufHashInit(Size init_size, Size max_size, HTAB ** hash_table_dptr, 
				Size tag_size, Size ent_size, int part_num, const char* hname,
				bool found)
{
	HashInit(init_size, max_size, hash_table_dptr, tag_size, ent_size, part_num, 
					 hname, found);
}

uint32
NDPDBufHashCode(const void * keydbuf, HTAB * hash_table_ptr)
{
	return HashGetCode(keydbuf, hash_table_ptr);
}

DBufLookupEnt*
NDPHashLookup(const void * keydbuf, uint32 hashcode, HTAB * hash_table_ptr)
{
	bool found;
	DBufLookupEnt * result = (DBufLookupEnt*)
		HashLookup((const void*)keydbuf, hashcode, hash_table_ptr, &found);

	if (found)
		return result;
	
	return NULL;
}

DBufLookupEnt*
NDPHashInsert(DBufTag* keydbuf, uint32 hashcode, HTAB *hash_table_ptr)
{
	bool found;
	DBufLookupEnt * result = (DBufLookupEnt*)
		HashInsert((const void*)keydbuf, hashcode, hash_table_ptr, &found);

	if (found)
	{
		return result;
	}

	pg_memory_barrier();

	result->dbufidlist.num_elem = 0;
	result->dbufidlist.flush_times = 0;
	result->dbufidlist.head_node_idx = INVALIDIDX;
	result->dbufidlist.tail_node_idx = INVALIDIDX;
	result->dbufidlist.global_lock_number = buf_id_set_desc->alloced_number;
	buf_id_set_desc->alloced_number++;

#ifdef SMARTSSD_DBUF_DEBUG
	fprintf(stderr, "Insert Rel[%u][%u] to %p with list lock %d %p\n", 
			keydbuf->relid, keydbuf->segno, &result->dbufidlist, 
			result->dbufidlist.global_lock_number, 
			DBufDListLock(result->dbufidlist.global_lock_number));
#endif

	return result;
}

bool
NDPHashDelete(const void *keydbuf, uint32 hashcode, HTAB *hash_table_ptr)
{
	bool found = false;
	HashDelete((const void*)keydbuf, hashcode, hash_table_ptr, &found);

	return found;
}

/*
 * NDPBufIDSetShmemSize
 * 
 * Get shared memory size for dirty buffer id lists(buf_id_set_desc)
 * It just has the number of cached heap relation, 
 * the current maximum number of cached heap relation(It is expanded, if 
 * it need to cache more relation), and the dynamic shared memory pointer of 
 * dirty buffer ID's lists.
 * Except for these, other data and metadata is set within dynamic shared memory
 * followed by theirs usage.
 * So fixed size of dirty buf id lists is 16(sizeof(int) * 2 sizeof(ptr)) bytes.
 */
Size
NDPBufIDSetShmemSize(void)
{
	Size size = 0;

	// Set buf_id_set_desc size to shared memory	
	size = add_size(size, sizeof(BufIDSetData));
	size = add_size(size, sizeof(BufIDNodeData) * (FSTACKSIZE) + 
								 PG_CACHE_LINE_SIZE);

	size = add_size(size, NDPDBufHashShmemSize(SOFTMAXNUMREL + 
						NUM_DBUF_HEAPID_HASH, sizeof(TTLookupEnt)));
	size = add_size(size, NDPDBufHashShmemSize(INITNUMREL * INITNUMSEG * 32 +
						NUM_DBUF_CHAIN_ALLOC, sizeof(DBufLookupEnt)));

	// Return size of buf_id_set_desc
	return size;
}

/*
 * NDPBufIDSetInit
 *
 * Initialize the shared memory for dirty buffer id lits.
 */
void
NDPBufIDSetInit(void)
{
	bool found;
	char *ptr;
	Size space_freenode = sizeof(BufIDNodeData) * (FSTACKSIZE) 
						  + PG_CACHE_LINE_SIZE;
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	/* Need to initialize. initialize it */
	buf_id_set_desc = 
		(BufIDSet)ShmemInitStruct("Dirty Buffer IDs List Descriptor",
		sizeof(BufIDSetData), &found);

	if (!found)
	{
#ifdef SMARTSSD_DBUF_DEBUG
		fprintf(stderr, "Initializing DBufIDSet Descriptor\n");	
#endif
		memset(buf_id_set_desc, 0, sizeof(BufIDSetData));
	}

#ifdef SMARTSSD_DBUF_DEBUG
	fprintf(stderr, "ndp_stack_base = %p\n", ndp_stack_base);
#endif

	if (!IsUnderPostmaster)
	{
		ptr = (char *) ShmemAlloc(space_freenode);
		memset(ptr, 0, space_freenode);
		ptr += PG_CACHE_LINE_SIZE - ((uintptr_t) ptr) % PG_CACHE_LINE_SIZE;

		ndp_stack_base = (BufIDNode) ptr;

#ifdef SMARTSSD_DBUF_DEBUG
		fprintf(stderr, "ndp_stack_base = %p\n", ndp_stack_base);
		fprintf(stderr, "Initializing free node stack\n");	
#endif
		NDPInitFreeNodeStack();
	}
	NDPDBufHashInit(SOFTMAXNUMREL + NUM_DBUF_HEAPID_HASH,
					SOFTMAXNUMREL + NUM_DBUF_HEAPID_HASH, &TargetTableIDHash, 
					sizeof(TTTag), sizeof(TTLookupEnt), NUM_DBUF_HEAPID_HASH, 
					"NDP Target Table ID Hash", found);
	NDPDBufHashInit(INITNUMREL * INITNUMSEG * 32 + NUM_DBUF_CHAIN_ALLOC, 
					INITNUMREL * INITNUMSEG * 32 + NUM_DBUF_CHAIN_ALLOC,
					&SharedBufIDSetHash, sizeof(DBufTag), sizeof(DBufLookupEnt),
					NUM_DBUF_CHAIN_ALLOC, "Shared Dirty Buf ID Set Table", 
					found);

	MdCxt_dbuflist = AllocSetContextCreate(TopMemoryContext, "DbufListPerSeg",
										   ALLOCSET_DEFAULT_SIZES);

	pg_memory_barrier();
	LWLockRelease(AddinShmemInitLock);
}

/*
 * NDPInitFreeNodeStack
 *
 * Buf Id node allocate first and it is managed in free list
 */
void 
NDPInitFreeNodeStack(void)
{
	FreeDBufNode cur_free_node;
	int idx;
	LWLock * fstack_lock = DBufFStackLock();

	Assert(!buf_id_set_desc->free_node_stack.stack_head_idx);
	Assert(sizeof(BufIDNodeData) == sizeof(FreeDBufNodeData));

	LWLockAcquire(fstack_lock, LW_EXCLUSIVE);

	cur_free_node = (FreeDBufNode)ndp_stack_base;

	for (idx = 1 ; idx < FSTACKSIZE ; idx++)
	{
		cur_free_node->prev_idx = idx;
		cur_free_node++;
	}
	cur_free_node->prev_idx = INVALIDIDX;
#ifdef SMARTSSD_DBUF_DEBUG
	buf_id_set_desc->free_node_stack.num_elem = FSTACKSIZE;
#endif

	LWLockRelease(fstack_lock);
}

/*
 * NDPPopFreeNode
 *
 * Pop free node from stack and get its pointer
 * If stack is empty, then assert
 */
static BufIDNode 
NDPPopFreeNode(void)
{
	BufIDNode free_node;
	FreeDBufNode cur_free_node;
	int prev_idx, head_idx;
	
	while (true)
	{
		head_idx = buf_id_set_desc->free_node_stack.stack_head_idx;
#ifdef SMARTSSD_DBUF_DEBUG
		Assert(buf_id_set_desc->free_node_stack.stack_head_idx >= 0 &&
			   buf_id_set_desc->free_node_stack.stack_head_idx < FSTACKSIZE);
		Assert(buf_id_set_desc->free_node_stack.num_elem);

		if (head_idx == INVALIDIDX)
		{
			elog(PANIC, "there's no node in stack\n");
		}
#endif
		cur_free_node = (FreeDBufNode)(&ndp_stack_base[head_idx]);
		prev_idx = cur_free_node->prev_idx;
		if (head_idx != buf_id_set_desc->free_node_stack.stack_head_idx)
		{
			continue;
		}
		if (prev_idx == INVALIDIDX)
		{
			continue;
		}
		if (true ==
			pg_atomic_compare_exchange_u32((volatile pg_atomic_uint32*)
			(&buf_id_set_desc->free_node_stack.stack_head_idx),
			(uint32*)(&head_idx), (uint32)prev_idx))
		{
			free_node = (BufIDNode)cur_free_node;
#ifdef SMARTSSD_DBUF_DEBUG
			pg_atomic_fetch_sub_u32((volatile pg_atomic_uint32*)
			(&buf_id_set_desc->free_node_stack.num_elem), 1);
#endif
			free_node->prev_node_idx = INVALIDIDX;
			free_node->next_node_idx = INVALIDIDX;

			return free_node;
		}
	}

	Assert(false);
	return NULL;
#if 0
	LWLock * fstack_lock = DBufFStackLock();

	LWLockAcquire(fstack_lock, LW_EXCLUSIVE);
	
	pg_memory_barrier();
#ifdef SMARTSSD_DBUF_DEBUG
	if (buf_id_set_desc->free_node_stack.stack_head_idx == INVALIDIDX ||
		buf_id_set_desc->free_node_stack.num_elem == 0)
	{
		elog(PANIC, "there's no node in stack\n");
	}
#endif
	
	free_node = 
		&ndp_stack_base[buf_id_set_desc->free_node_stack.stack_head_idx];
	
	buf_id_set_desc->free_node_stack.stack_head_idx = 
		((FreeDBufNode)free_node)->prev_idx;

#ifdef SMARTSSD_DBUF_DEBUG
	buf_id_set_desc->free_node_stack.num_elem--;
#endif

	free_node->next_node_idx = free_node->prev_node_idx = INVALIDIDX;

	pg_memory_barrier();
	LWLockRelease(fstack_lock);

	return free_node;
#endif
}

/*
 * NDPPushFreeNode
 *
 * push free node to stack
 */
void
NDPPushFreeNode(FreeDBufNode free_node)
{
	int cur_idx, head_idx;	

	cur_idx = (int)(((uint64)free_node - (uint64)ndp_stack_base) / 	
				sizeof(FreeDBufNodeData));	

#ifdef SMARTSSD_DBUF_DEBIG	
	Assert(buf_id_set_desc->free_node_stack.num_elem < FSTACKSIZE);	
#endif	

	memset(free_node, 0, sizeof(FreeDBufNodeData));	

	while (true)	
	{	
		head_idx = buf_id_set_desc->free_node_stack.stack_head_idx;	
		free_node->prev_idx = head_idx;	

		if (free_node->prev_idx !=	
			buf_id_set_desc->free_node_stack.stack_head_idx)	
		{	
			continue;	
		}	

		if (true == 	
			pg_atomic_compare_exchange_u32((volatile pg_atomic_uint32*)	
			(&buf_id_set_desc->free_node_stack.stack_head_idx), 	
			(uint32*)(&head_idx), (uint32)cur_idx))	
		{	
#ifdef SMARTSSD_DBUF_DEBUG	
			pg_atomic_fetch_add_u32((volatile pg_atomic_uint32*)	
			(&buf_id_set_desc->free_node_stack.num_elem), 1);	
#endif	
			return;	
		}	
	}
#if 0
	LWLock * fstack_lock = DBufFStackLock();
	LWLockAcquire(fstack_lock, LW_EXCLUSIVE);
	pg_memory_barrier();

	((BufIDNode)free_node)->buf_id = -1;
	((BufIDNode)free_node)->prev_node_idx = -1;
	((BufIDNode)free_node)->next_node_idx = -1;
	((BufIDNode)free_node)->blk_num = 0;

#ifdef SMARTSSD_DBUF_DEBUG
	Assert(buf_id_set_desc->free_node_stack.num_elem < FSTACKSIZE);
#endif

	free_node->prev_idx = buf_id_set_desc->free_node_stack.stack_head_idx;
	buf_id_set_desc->free_node_stack.stack_head_idx = 
		(int)(((uint64)free_node - (uint64)ndp_stack_base) / 
		sizeof(FreeDBufNodeData));
	Assert(buf_id_set_desc->free_node_stack.stack_head_idx >= 0 &&
				 buf_id_set_desc->free_node_stack.stack_head_idx < FSTACKSIZE);

#ifdef SMARTSSD_DBUF_DEBUG
	buf_id_set_desc->free_node_stack.num_elem++;
#endif

	pg_memory_barrier();

	LWLockRelease(fstack_lock);
#endif 
}

#ifdef SMARTSSD_DBUF_DEBUG
/*
 * NDPPushFreeNode
 *
 * push free node to stack
 */
static bool
CheckIsInFreeNode(int checked_idx)
{
	int cur_idx;
	FreeDBufNode free_node;
	int stack_num = 0;
	LWLock * fstack_lock = DBufFStackLock();

	LWLockAcquire(fstack_lock, LW_EXCLUSIVE);
	cur_idx = buf_id_set_desc->free_node_stack.stack_head_idx;
	
	while (cur_idx != INVALIDIDX)
	{
		if (cur_idx == checked_idx)
		{
			LWLockRelease(fstack_lock);
			return true;
		}

		free_node = (FreeDBufNode)(&ndp_stack_base[cur_idx]);
		stack_num++;
		cur_idx = free_node->prev_idx;
	}
	
	fprintf(stderr, "stack node num : %d\n", stack_num);

	LWLockRelease(fstack_lock);
	return false;
}
#endif

/*
 * NDPSetDirtyBufID
 *
 * When page becomes dirty, this function is called to put dirty buffer page id
 * in buf id set array
 */
#ifdef SMARTSSD_DBUF_DEBUG
void 
NDPSetDirtyBufID(volatile DirtyBufIDList dirty_bufid_list, Buffer buf, 
				 uint32 relid, uint32 segno)
#else
void 
NDPSetDirtyBufID(volatile DirtyBufIDList dirty_bufid_list, Buffer buf)
#endif
{
	BufIDNode dbuf_id_node, cur_tail_id_node;
	int dbuf_id_node_idx;

	// Already managed by dirty buf id list, we don't need to push it to list.
	if (GetBufferDescriptor(buf - 1)->dirty_id_node_idx != INVALIDIDX)
	{
#ifdef SMARTSSD_DBUF_DEBUG
		if (!IsPointingRightNode(GetBufferDescriptor(buf - 1)->dirty_id_node_idx,
				GetBufferDescriptor(buf - 1)->buf_id))
		{
			int err_node_idx = GetBufferDescriptor(buf - 1)->dirty_id_node_idx;
			BufIDNode test_node = &ndp_stack_base[err_node_idx];
			int idx_from_err_node = 
				GetBufferDescriptor(test_node->buf_id - 1)->dirty_id_node_idx;
			BufIDNode node_from_err_idx = &ndp_stack_base[idx_from_err_node];
			int err_dst_buf = node_from_err_idx->buf_id;

			fprintf(stderr, "cur_dnode_idx : %d\n", 
					GetBufferDescriptor(buf - 1)->dirty_id_node_idx);
			fprintf(stderr, "errnode_bufid : %d cur buf id %d\n", 
					test_node->buf_id - 1, 
					GetBufferDescriptor(buf - 1)->buf_id);

			fprintf(stderr, "DList Flush Time : %d\n", 
					dirty_bufid_list->flush_times);
			fprintf(stderr, "Buffer Flush Time : %d\n", 
					GetBufferDescriptor(buf - 1)->flush_times);

			fprintf(stderr, "buf[%d] Rel[%u] [%u]\n", buf, relid, segno);
			fprintf(stderr, "Err buf[%d] Rel[%u][%u]\n", err_dst_buf,
					GetBufferDescriptor(err_dst_buf - 1)->tag.rnode.relNode,
					GetBufferDescriptor(err_dst_buf - 1)->tag.blockNum / RELSEG_SIZE);

			fprintf(stderr, "err_node_idx : %d\n", err_node_idx);
			fprintf(stderr, "idx_from_err_node : %d\n",	idx_from_err_node);
			Assert(0);
		}
#endif //SMARTSSD_DBUF_DEBUG
		if (!IsPointingRightNode(
				GetBufferDescriptor(buf - 1)->dirty_id_node_idx,
				GetBufferDescriptor(buf - 1)->buf_id))
		{
			uint32 state = GetBufferDescriptor(buf - 1)->state.value;
			fprintf(stderr, "Dirty : %u\n", state & BM_DIRTY);
			fprintf(stderr, "Valid : %u\n", state & BM_VALID);
			fprintf(stderr, "Tag Valid : %u\n", state & BM_TAG_VALID);
			fprintf(stderr, "IO ERROR : %u\n", state & BM_IO_ERROR);
			fprintf(stderr, "CHECKPOINT NEEDED: %u\n", state & BM_CHECKPOINT_NEEDED);
			fprintf(stderr, "PERMANENT : %u\n", state & BM_PERMANENT);
			Assert(0);
		}
		pg_memory_barrier();
		return;
	}

	// set data to node
	pg_memory_barrier();
	dbuf_id_node = NDPPopFreeNode();

	dbuf_id_node->buf_id = buf;
	dbuf_id_node->blk_num = GetBufferDescriptor(buf - 1)->tag.blockNum;

	dbuf_id_node_idx = (int)(((uint64)dbuf_id_node - (uint64)ndp_stack_base) / 
						sizeof(BufIDNodeData));

#ifdef SMARTSSD_DBUF_DEBUG
	dbuf_id_node->relid = relid;
	dbuf_id_node->segno = segno;
#endif

	// there's not cached node in segment
	if (dirty_bufid_list->head_node_idx == INVALIDIDX)
	{
		Assert(dirty_bufid_list->tail_node_idx == INVALIDIDX);
		Assert(!dirty_bufid_list->num_elem);

		// set node pointer in head
		dirty_bufid_list->head_node_idx = dbuf_id_node_idx;
	}
	else
	{
		Assert(dirty_bufid_list->tail_node_idx != INVALIDIDX);
		Assert(dirty_bufid_list->num_elem); //

		// before changing tail, firstly tail's next node set to new node
		dbuf_id_node->prev_node_idx = dirty_bufid_list->tail_node_idx;

		// get current tail node
		cur_tail_id_node = &ndp_stack_base[dirty_bufid_list->tail_node_idx];
		cur_tail_id_node->next_node_idx = dbuf_id_node_idx;
	}

	// change tail
	dirty_bufid_list->tail_node_idx = dbuf_id_node_idx;

	pg_memory_barrier();
	dirty_bufid_list->num_elem++;
	pg_memory_barrier();
		
#ifdef SMARTSSD_DBUF_DEBUG
	if (dirty_bufid_list->num_elem != 
		GetNodeNumInDList(dirty_bufid_list->head_node_idx, 
						  dirty_bufid_list->tail_node_idx, false))
	{
		GetNodeNumInDList(dirty_bufid_list->head_node_idx,
						  dirty_bufid_list->tail_node_idx, true);
		fprintf(stderr, "%d\n", dirty_bufid_list->num_elem);	
		Assert(0);	
	}
#endif

	GetBufferDescriptor(buf - 1)->dirty_id_node_idx = dbuf_id_node_idx;

	pg_memory_barrier();
}

/*
 * NDPPopDirtyBufID
 *
 * When page becomes dirty, this function is called to extract  dirty buffer 
 * page id in buf id set array
 */
#ifdef SMARTSSD_DBUF_DEBUG
void 
NDPPopDirtyBufID(volatile DirtyBufIDList dirty_bufid_list, Buffer buf, 
				 Oid relid, Oid segno)
#else
void 
NDPPopDirtyBufID(volatile DirtyBufIDList dirty_bufid_list, Buffer buf)
#endif
{
	BufIDNode dbuf_id_node, next_dbuf_id_node, prev_dbuf_id_node;
	int pop_node_idx = GetBufferDescriptor(buf - 1)->dirty_id_node_idx;

	GetBufferDescriptor(buf - 1)->dirty_id_node_idx = INVALIDIDX;

	// If other threads flush this page.
	// Then, just release this page
	if (pop_node_idx == INVALIDIDX)
	{
		return;
	}
	
	Assert(pop_node_idx >= 0 && pop_node_idx < FSTACKSIZE);

#ifdef SMARTSSD_DBUF_DEBUG
		if (!IsPointingRightNode(pop_node_idx,
			GetBufferDescriptor(buf - 1)->buf_id))
		{
			BufIDNode test_node = &ndp_stack_base[pop_node_idx];

			fprintf(stderr, "%d %d\n", test_node->buf_id, 
					GetBufferDescriptor(buf - 1)->buf_id);
			Assert(0);
		}
#endif //SMARTSSD_DBUF_DEBUG

	// get address of buf id node
	dbuf_id_node = &ndp_stack_base[pop_node_idx];

#ifdef SMARTSSD_DBUF_DEBUG
	Assert(dbuf_id_node->relid == 
			GetBufferDescriptor(buf - 1)->tag.rnode.relNode);
	Assert(dbuf_id_node->segno == 
			GetBufferDescriptor(buf - 1)->tag.blockNum / RELSEG_SIZE);
	Assert(dbuf_id_node->blk_num ==
			GetBufferDescriptor(buf - 1)->tag.blockNum);
#endif

	Assert(dirty_bufid_list->num_elem >= 0 && 
		   dirty_bufid_list->num_elem < FSTACKSIZE);

	// Just this node only inserted in here 
	if (dirty_bufid_list->head_node_idx == pop_node_idx &&
		dirty_bufid_list->tail_node_idx == pop_node_idx)
	{
		dirty_bufid_list->head_node_idx = INVALIDIDX;
		dirty_bufid_list->tail_node_idx = INVALIDIDX;

		Assert(dbuf_id_node->next_node_idx == INVALIDIDX);
		Assert(dbuf_id_node->prev_node_idx == INVALIDIDX);
	}
	// if this node is head node
	else if (dirty_bufid_list->head_node_idx == pop_node_idx)
	{
		dirty_bufid_list->head_node_idx = dbuf_id_node->next_node_idx;
		Assert(dbuf_id_node->next_node_idx != INVALIDIDX);
		Assert(dbuf_id_node->prev_node_idx == INVALIDIDX);
		next_dbuf_id_node = &ndp_stack_base[dbuf_id_node->next_node_idx];
		next_dbuf_id_node->prev_node_idx = INVALIDIDX;
	}
	// if this node is tail node
	else if (dirty_bufid_list->tail_node_idx == pop_node_idx)
	{
		dirty_bufid_list->tail_node_idx = dbuf_id_node->prev_node_idx;
		Assert(dbuf_id_node->prev_node_idx != INVALIDIDX);
		Assert(dbuf_id_node->next_node_idx == INVALIDIDX);
		prev_dbuf_id_node = &ndp_stack_base[dbuf_id_node->prev_node_idx];
		prev_dbuf_id_node->next_node_idx = INVALIDIDX;
	}
	// if this node is in a middle of list 
	else
	{
#ifdef SMARTSSD_DBUF_DEBUG
		if (dbuf_id_node->next_node_idx == INVALIDIDX ||
			dbuf_id_node->prev_node_idx == INVALIDIDX)
		{
			BufIDNode chk_node;
			int chk_idx = dirty_bufid_list->head_node_idx;
			int chk_num = 0;
			bool in_list = false;
			
			fprintf(stderr, "Rel[%u] : seg[%u]\n", relid, segno);
			if (CheckIsInFreeNode(pop_node_idx))
			{
				fprintf(stderr, "This node is freed\n");
			}
			else
			{
				fprintf(stderr, "This node is non-freed\n");
			}
			fprintf(stderr, "%d %d\n", dirty_bufid_list->tail_node_idx,
					dirty_bufid_list->head_node_idx);
			fprintf(stderr, "%d %d\n", dbuf_id_node->next_node_idx,
					dbuf_id_node->prev_node_idx);
			fprintf(stderr, "buf[%d] : %d\n", buf, pop_node_idx);
			fprintf(stderr, "Idx's rel[%d] : %d\n", 
			  GetBufferDescriptor(dbuf_id_node->buf_id - 1)->tag.rnode.relNode,
			  GetBufferDescriptor(dbuf_id_node->buf_id - 1)->tag.blockNum /
								  RELSEG_SIZE);
			fprintf(stderr, "%d %d %d\n", buf, 
					GetBufferDescriptor(buf - 1)->buf_id, dbuf_id_node->buf_id);
		
			while (chk_idx != INVALIDIDX)
			{
				if (chk_idx == pop_node_idx)
					in_list = true;
				chk_num++;
				chk_node = &ndp_stack_base[chk_idx];	
				chk_idx = chk_node->next_node_idx;
			}
			fprintf(stderr, "chk_num : %d, list_num : %u\n", chk_num,
					dirty_bufid_list->num_elem);
			if (in_list)
				fprintf(stderr, "is in list");
			else
				fprintf(stderr, "not in list !!!!");

			chk_idx = pop_node_idx;
			chk_num = 0;
			
			while (chk_idx != INVALIDIDX)
			{
				chk_num++;
				chk_node = &ndp_stack_base[chk_idx];
				chk_idx = chk_node->prev_node_idx;
			}
			fprintf(stderr, "chk_num : %d, list_num : %u\n", chk_num,
					dirty_bufid_list->num_elem);

			chk_idx = pop_node_idx;
			chk_num = 0;
			
			while (chk_idx != INVALIDIDX)
			{
				chk_num++;
				chk_node = &ndp_stack_base[chk_idx];
				chk_idx = chk_node->next_node_idx;
			}

			fprintf(stderr, "chk_num : %d, list_num : %u\n", chk_num,
					dirty_bufid_list->num_elem);

			chk_node = &ndp_stack_base[dirty_bufid_list->tail_node_idx];
			if (chk_node->next_node_idx == INVALIDIDX)
			{
				fprintf(stderr, "Tail node is last node\n");
			}
			else
			{
				fprintf(stderr, "Tail node's next : %d\n", chk_node->next_node_idx);
				fprintf(stderr, "Cur node idx : %d\n", pop_node_idx);
			}

			fprintf(stderr, "bufrelnum : %u bufsegnum %u\n",
			GetBufferDescriptor(buf - 1)->tag.rnode.relNode,
	 		GetBufferDescriptor(buf - 1)->tag.blockNum / RELSEG_SIZE);

			if (pop_node_idx == 0)
			{
				chk_node = &ndp_stack_base[pop_node_idx + 1];
				chk_idx = pop_node_idx + 1;
				fprintf(stderr, "%d %d %d\n", chk_idx, 
						GetBufferDescriptor(chk_node->buf_id + 1)->buf_id,
						chk_node->buf_id);
			}
			else if (pop_node_idx == FSTACKSIZE - 1)
			{
				chk_node = &ndp_stack_base[pop_node_idx - 1];
				chk_idx = pop_node_idx - 1;
				fprintf(stderr, "%d %d %d\n", chk_idx, 
						GetBufferDescriptor(chk_node->buf_id + 1)->buf_id,
						chk_node->buf_id);
			}
			else
			{
				chk_node = &ndp_stack_base[pop_node_idx + 1];
				chk_idx = pop_node_idx + 1;
				fprintf(stderr, "%d %d %d\n", chk_idx, 
						GetBufferDescriptor(chk_node->buf_id)->buf_id,
						chk_node->buf_id);

				chk_node = &ndp_stack_base[pop_node_idx - 1];
				chk_idx = pop_node_idx - 1;
				fprintf(stderr, "%d %d %d\n", chk_idx, 
						GetBufferDescriptor(chk_node->buf_id)->buf_id,
						chk_node->buf_id);
			}

			fprintf(stderr, "Error Rel[%u][%u] to %p\n", relid, segno, 
					dirty_bufid_list);
			Assert(0);
		}
#endif
		prev_dbuf_id_node = &ndp_stack_base[dbuf_id_node->prev_node_idx];
		prev_dbuf_id_node->next_node_idx = dbuf_id_node->next_node_idx;
		next_dbuf_id_node = &ndp_stack_base[dbuf_id_node->next_node_idx];
		next_dbuf_id_node->prev_node_idx = dbuf_id_node->prev_node_idx;
	}

	pg_memory_barrier();
	dirty_bufid_list->num_elem--;
	pg_memory_barrier();
		
#ifdef SMARTSSD_DBUF_DEBUG
	if (dirty_bufid_list->num_elem != 
		GetNodeNumInDList(dirty_bufid_list->head_node_idx, 
		dirty_bufid_list->tail_node_idx, false))
	{
		GetNodeNumInDList(dirty_bufid_list->head_node_idx,
		dirty_bufid_list->tail_node_idx, true);

		fprintf(stderr, "%d\n", dirty_bufid_list->num_elem);	
		GetNodeNumInDList(dbuf_id_node->prev_node_idx,
		dbuf_id_node->next_node_idx, true);

		Assert(0);	
	}
#endif

	pg_memory_barrier();
	// free this node
	NDPPushFreeNode((FreeDBufNode)dbuf_id_node);
}

/*
 * NDPDetachDirtyBufID
 *
 * For flushing dirty page ID's list, detach current list of dirty buffer IDs
 * After makeing list to array return it.
 */
FlushDirtyBufIDList
NDPDetachDirtyBufIDs(volatile DirtyBufIDList dirty_bufid_list, Oid relid, uint32 segno, 
										 LWLock * list_content_lock)
{
	FlushDirtyBufIDList flush_dirty_bufid_list;
	int head_node_idx, cur_node_idx; 
	// int del_node_idx;
#ifdef SMARTSSD_DBUF_DEBUG
	int tail_node_idx;
#endif
	BufIDNode cur_node;

	uint cur_num_elem = 0;

	// If there's any other buf id is cached, just return NULL
	if (!dirty_bufid_list->num_elem)
	{
		Assert(dirty_bufid_list->head_node_idx == INVALIDIDX);
		Assert(dirty_bufid_list->tail_node_idx == INVALIDIDX);

#ifdef SMARTSSD_DBUF_DEBUG
		fprintf(stderr, "Rel[%u][%u] doesn't have any elem\n", relid, segno);
#endif
		
		pg_memory_barrier();
		LWLockRelease(list_content_lock);
		
		return NULL;
	}

	pg_memory_barrier();
	flush_dirty_bufid_list = MemoryContextAlloc(MdCxt_dbuflist,
							 sizeof(FlushDirtyBufIDListData));

	// Get current number of elemts
	flush_dirty_bufid_list->num_elem = dirty_bufid_list->num_elem;
	// Get fetch and add flush times

	head_node_idx = dirty_bufid_list->head_node_idx; 
#ifdef SMARTSSD_DBUF_DEBUG
	tail_node_idx = dirty_bufid_list->tail_node_idx; 
#endif

	flush_dirty_bufid_list->dirty_buf_ids_arr = 
		(int*)MemoryContextAlloc(MdCxt_dbuflist, sizeof(int) *
								 flush_dirty_bufid_list->num_elem);

	flush_dirty_bufid_list->blk_num_arr = 
		(BlockNumber*)MemoryContextAlloc(MdCxt_dbuflist, sizeof(BlockNumber) *
										 flush_dirty_bufid_list->num_elem);


	pg_memory_barrier();

	Assert(head_node_idx != INVALIDIDX);
#ifdef SMARTSSD_DBUF_DEBUG
	Assert(tail_node_idx != INVALIDIDX);
#endif

	cur_node_idx = head_node_idx; 

	//Make lists to array
	while (cur_node_idx != INVALIDIDX)
	{
		Assert(dirty_bufid_list->num_elem >= 0 && 
			   dirty_bufid_list->num_elem < FSTACKSIZE);
		Assert(cur_num_elem < flush_dirty_bufid_list->num_elem);

		cur_node = &ndp_stack_base[cur_node_idx];

		Assert(BufferIsValid(cur_node->buf_id));
		flush_dirty_bufid_list->dirty_buf_ids_arr[cur_num_elem] = 
			cur_node->buf_id;

		flush_dirty_bufid_list->blk_num_arr[cur_num_elem] = cur_node->blk_num;
		cur_num_elem++;

		cur_node_idx = cur_node->next_node_idx;
	}

	pg_memory_barrier();

	Assert(cur_num_elem == flush_dirty_bufid_list->num_elem);

	LWLockRelease(list_content_lock);

	flush_dirty_bufid_list->relid = relid;
	flush_dirty_bufid_list->segno = segno;


#ifdef SMARTSSD_DBUF_DEBUG
	fprintf(stderr, "%d %u FLUSH_LIST_GEN\n", relid, segno);
#endif

	//return the pointer of list
	// this pointer will be freed in FlushSegmentsForNDP
	return flush_dirty_bufid_list;
}

/*
 * NDPGetDPtrDBufIDsList
 *
 * 6/11
 * Get dsa_pointer of relation segment.
 * If cannot find relation in dirty buf ids list and need not allocate,
 * return InvalidDsaPointer
 */
bool
IsTargetBuffer(Oid relid)
{ 
	bool found;
	uint32 hashcode = NDPDBufHashCode(&relid, TargetTableIDHash);
	LWLock * target_checking_lock = DBufHeapIDLock();
	
	LWLockAcquire(target_checking_lock, LW_SHARED);
		
	HashLookup(&relid, hashcode, TargetTableIDHash, &found);

	LWLockRelease(target_checking_lock);

	return found;
}

/*
 * Functions for debugging
 * Checking if node pointer and buf id used by node pointer is same
 */
static bool
IsPointingRightNode(int test_node_idx, int test_buf_id)
{
	BufIDNode test_node = &ndp_stack_base[test_node_idx];
	return (test_node->buf_id - 1 == test_buf_id);
}

#ifdef SMARTSSD_DBUF_DEBUG
void
IsPointingRightNode2(int test_node_idx, int test_buf_id)
{
	BufIDNode test_node = &ndp_stack_base[test_node_idx];
	BufferDesc * bufHdr = GetBufferDescriptor(test_buf_id);

	Assert(test_node->buf_id == test_buf_id);
	Assert(test_node->blk_num == bufHdr->tag.blockNum);
	Assert(test_node->relid == bufHdr->tag.rnode.relNode);
}


static int
GetNodeNumInDList(int head_node_idx, int prev_node_idx, bool verbose)
{
	BufIDNode cur_node;
	int num_from_front = 0, num_from_rear = 0;
	int cur_node_idx = head_node_idx; 

	//Make lists to array
	while (cur_node_idx != INVALIDIDX)
	{
		cur_node = &ndp_stack_base[cur_node_idx];
		num_from_front++;
		cur_node_idx = cur_node->next_node_idx;
	}

	cur_node_idx = prev_node_idx;
	//Make lists to array
	while (cur_node_idx != INVALIDIDX)
	{
		cur_node = &ndp_stack_base[cur_node_idx];
		num_from_rear++;
		cur_node_idx = cur_node->prev_node_idx;
	}

	if (verbose)
	{
		fprintf(stderr, "From head : %d\n", num_from_front);
		fprintf(stderr, "From tail : %d\n", num_from_rear);
	}
	else
	{
		Assert(num_from_front == num_from_rear);
	}
	return num_from_front;
}

#endif

#endif //if defined(SMARTSSD_DBUF)
