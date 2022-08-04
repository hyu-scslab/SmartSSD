/*---------------------------------------------------------------------
 *
 *
 * ndp_support_hash.c
 *    routines to handle hash for ndp support module.
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *    src/backend/storage/ssd/ndp_support_hash.c
 *---------------------------------------------------------------------
 */

#include "storage/ndp_support_hash.h"

#ifdef SMARTSSD_DBUF

#include "storage/lwlock.h"
#include "storage/shmem.h"

/*
 * GetHashShmemSize
 *
 * compute the size of hash table in shared memory
 */
Size
HashGetShmemSize(int size, size_t entry_size)
{
  return hash_estimate_size(size, entry_size);
}

/*
 * HashInit
 *
 * Initialize hash table in shared memory
 */
void
HashInit(int init_size, int max_size, HTAB ** hash_table_ptr, size_t key_size, 
				 size_t entry_size, int partition_number, const char* hash_desc, 
				 bool found)
{
  HASHCTL   info;
	int hash_flags;
	
	if (partition_number >= 0 && partition_number <= 1)
	{
		partition_number = 0;
	}

  /* Version maps to version entry */
  info.keysize = key_size;
  info.entrysize = entry_size;
  info.num_partitions = partition_number;
	hash_flags = HASH_ELEM | HASH_BLOBS;
	if (partition_number)
		hash_flags |= HASH_PARTITION;

  *hash_table_ptr = ShmemInitHash(hash_desc, init_size, max_size, &info, 
																	hash_flags);
}

/*
 * HashGetCode
 *
 * Compute the hash value associated with a RelvTag
 * This must be passed to the lookup/insert/delete routines along with the
 * tag. We do it like this because the callers need to know the hash code
 * in order to determine which buffer partition to lock, and we don't want
 * to do the hash computation twice (hash_any is a bit slow).
 */
uint32
HashGetCode(const void *keyPtr, HTAB * hash_table_ptr)
{
  return get_hash_value(hash_table_ptr, keyPtr);
}

/*
 * LookupHash
 *
 * Lookup the given ptr; return pointer or NULL if not found
 * Caller must hold at lesat share lock for tag's partition
 */
void* 
HashLookup(const void *keyPtr, uint32 hashcode, HTAB * hash_table_ptr, 
           bool *found)
{
  void *result = hash_search_with_hash_value(hash_table_ptr, keyPtr, hashcode,
                                             HASH_FIND, found);

  return result;
}

/*
 * HashInsert
 *
 * Insert a hashtable entry for given key and key location,
 * unless an entry already exists for that key.
 *
 * Returns -1 on successful insertion. If a conflicting entry exists
 * already, returns the ID in that entry.
 *
 * Caller must hold exclusive lock on RelVEntryMappingLock for tag's partition
 */
void *
HashInsert(const void *keyPtr, uint32 hashcode, HTAB * hash_table_ptr, 
           bool *found)
{
  void *result = hash_search_with_hash_value(hash_table_ptr, keyPtr, hashcode,
                			                       HASH_ENTER, found);
  return result;
}

/*
 * HashDelete
 *
 * Delete a hashtable entry for given key,
 * Caller must hold exclusive lock on lock for key's partition.
 */
void *
HashDelete(const void *keyPtr, uint32 hashcode, HTAB * hash_table_ptr, 
           bool *found)
{

  return hash_search_with_hash_value(hash_table_ptr, keyPtr, hashcode, HASH_REMOVE, 
                                     NULL);

}

#endif //ifdef SMARTSSD
