/*-------------------------------------------------------------------------
 *
 * vi_hash.c
 *	  Hash table implementation for mapping ViBufferBlocks to ViBuffer indexes.
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/storage/vi/vi_hash.c
 *
 *-------------------------------------------------------------------------
 */
#ifdef VERSION_INDEX
#include "postgres.h"

#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/dynahash.h"
#include "utils/hsearch.h"
#include "utils/snapmgr.h"
#include "storage/block.h"
#include "storage/bufpage.h"

#include "storage/vi.h"
#include "storage/vi_bufpage.h"
#include "storage/vi_buf.h"
#include "storage/vi_hash.h"

typedef struct
{
	ViBufferTag key; /* Tag of a disk page */
	int			id;  /* Associated buffer id */
} ViLookupEnt;

static HTAB *SharedViHash;

/*
 * ViHashShmemSize
 *
 * 		Compute the size of shared memory for version index hash.
 */
Size
ViHashShmemSize(int size)
{
	return hash_estimate_size(size, sizeof(ViLookupEnt));
}

/*
 * ViHashInit
 *
 * 		Initialize version index hash in shared memory.
 */
void
ViHashInit(int size)
{
	HASHCTL info;
	long	num_partitions;

	/* See next_pow2_long(long num) in dynahash.c */
	num_partitions = 1L << my_log2(NUM_VI_BUFFER_PARTITIONS);

	/* EbiTreeBufTag maps to EbiTreeHash */
	info.keysize		= sizeof(ViBufferTag);
	info.entrysize		= sizeof(ViLookupEnt);
	info.num_partitions = num_partitions;

	SharedViHash =
		ShmemInitHash("Shared Version Index Lookup Table", size, size, &info,
					  HASH_ELEM | HASH_BLOBS | HASH_PARTITION);
}

/*
 * ViHashCode
 * 		Compute the hash code associated with a ViBufferTag.
 * 		This must be passed to the lookup/insert/delete routines along with the
 * 		tag. We do this way because the callers need to know the hash code in
 * 		order to determine which buffer partition to lock, and we don't want to
 * 		do the hash computation twice (hash_any is a bit slow).
 */
uint32
ViHashCode(const ViBufferTag *tagPtr)
{
	return get_hash_value(SharedViHash, (void *)tagPtr);
}

/*
 * ViHashLookup
 *
 * 		Lookup the given ViBufferTag; return version index page id, or -1 if not
 * 		found Caller must hold at least shared lock on ViMappingLock for tag's
 * 		partition
 */
int
ViHashLookup(const ViBufferTag *tagPtr, uint32 hashcode)
{
	ViLookupEnt *result;

	result = (ViLookupEnt *)hash_search_with_hash_value(
		SharedViHash, (void *)tagPtr, hashcode, HASH_FIND, NULL);

	if (!result)
		return -1;

	return result->id;
}

/*
 * ViHashInsert
 *
 * 		Insert a hashtable entry for given tag and buffer id,
 * 		unless an entry already exists for that tag.
 *
 * 		Returns -1 on successful insertion. If a conflicting entry already
 * 		exists, returns its buffer ID.
 *
 * 		Caller must hold exclusive lock on EbiTreeMappingLock for
 * 		tag's partition.
 */
int
ViHashInsert(const ViBufferTag *tagPtr, uint32 hashcode, int buffer_id)
{
	ViLookupEnt *result;
	bool		 found;

	Assert(buffer_id >= 0); /* -1 is reserved for not-in-table */

	result = (ViLookupEnt *)hash_search_with_hash_value(
		SharedViHash, (void *)tagPtr, hashcode, HASH_ENTER, &found);

	if (found) /* found something already in the hash table */
		return result->id;

	result->id = buffer_id;
	return -1;
}

/*
 * ViHashDelete
 *
 * 		Delete the hashtable entry for given tag (must exist).
 *
 * 		Caller must hold exclusive lock on ViMappingLock for tag's partition.
 */
void
ViHashDelete(const ViBufferTag *tagPtr, uint32 hashcode)
{
	ViLookupEnt *result;

	result = (ViLookupEnt *)hash_search_with_hash_value(
		SharedViHash, (void *)tagPtr, hashcode, HASH_REMOVE, NULL);

	if (!result)
		elog(ERROR, "shared version index hash table corrupted");
}

#endif /* VERSION_INDEX */
