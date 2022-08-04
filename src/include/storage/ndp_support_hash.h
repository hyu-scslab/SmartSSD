/*-------------------------------------------------------------------------
 * ndp_support_hash.h
 * 
 * Interface of the hash for ndp support module 
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *    src/include/storage/ndp_support_hash.h
 *-------------------------------------------------------------------------
 */

#ifndef NDP_SUPPORT_HASH_H
#define NDP_SUPPORT_HASH_H

#ifdef SMARTSSD_DBUF

#include <stddef.h>

#include "postgres.h"

#include "utils/hsearch.h"

/* Get global hash table partition lock matched with hash value */
#define BufIDSetHashPartition(hash_value) \
  ((hash_value) % NUM_BUFIDSET_PARTITION)
#define BufIDSetHashPartitionLock(hash_value, rel_number) \
  (&MainLWLockArray[BUFID_HASH_LWLOCK_OFFSET + \
    (rel_number * NUM_BUFIDSET_PARTITION) + \
    BufIDSetHashPartition(hash_value)].lock)

Size HashGetShmemSize(int size, size_t entry_size);
void HashInit(int init_size, int max_size, HTAB ** hash_table_ptr, 
							size_t key_size, size_t entry_size, int partition_number, 
							const char* hash_desc, bool found);
uint32 HashGetCode(const void *keyPtr, HTAB * hash_table_ptr);
void* HashLookup(const void *keyPtr, uint32 hashcode, HTAB * hash_table_ptr,
                 bool *found);
void* HashInsert(const void *keyPtr, uint32 hashcode, HTAB * hash_table_ptr,
                 bool *found);
void* HashDelete(const void *keyPtr, uint32 hashcode, HTAB * hash_table_ptr, 
                 bool *found);

#endif //if defined(SMARTSSD)

#endif
