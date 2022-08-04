/*-------------------------------------------------------------------------
 *
 * vi_hash.h
 *	  Version index buffer hash functions.
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/vi_hash.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef VI_HASH_H
#define VI_HASH_H

#define ViHashPartition(hashcode) ((hashcode) % NUM_VI_PARTITIONS)

#define ViHashMappingPartitionLock(hashcode) \
( \
	&MainLWLockArray[VI_MAPPING_LWLOCK_OFFSET + \
  					 ViBufTableHashPartition(hashcode)].lock \
)

extern Size ViHashShmemSize(int size);
extern void ViHashInit(int size);
extern uint32 ViHashCode(const ViBufferTag *tagPtr);

extern int ViHashLookup(const ViBufferTag *tagPtr, uint32 hashcode);
extern int ViHashInsert(const ViBufferTag *tagPtr, uint32 hashcode,
						 int page_id);
extern void ViHashDelete(const ViBufferTag *tagPtr, uint32 hashcode);

#endif /* VI_HASH_H */
