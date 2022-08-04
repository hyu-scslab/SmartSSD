#ifdef SMARTSSD

#include "postgres.h"

#include "access/table.h"
#include "access/xact.h"
#include "common/relpath.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/lockdefs.h"
#include "storage/md.h"
#include "storage/relfilenode.h"
#include "storage/shmem.h"
#include "storage/smgr.h"
#include "utils/rel.h"
#include "utils/relcache.h"

#include "storage/ndp_version_index_cache.h"

#ifdef SMARTSSD_VI_DEBUG
#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#endif

extern PROC_HDR *ProcGlobal; /* For initilizing dsa module */

/* dsa module singleton for version index cache */
dsa_area *dsa_version_index;

/* in-memory version index cache singleton */
volatile VersionIndexCache in_memory_version_index_cache = NULL;

int VersionIndexFlushPerSegofRelType1(Relation rel, 
                                      VersionIndexCache vi_cache, 
                                      VersionIndexArrPerSegment vi_array,
                                      BlockNumber seg_idx);

static int FindUnusedRelVersionIndex(RelFileNode rnode);
static int FindRelVersionIndex(Relation rel);
#ifdef SMARTSSD_KT
static dsa_pointer GetDsaAreaForVersionIndexWithAlloc(pg_atomic_uint64* dst, 
                                                      dsa_pointer cmp_value, 
                                                      size_t allocation_size);
static dsa_pointer GetVersionIndexSetPerSegmentPtrType1(Relation rel,
                                VersionIndexCache vi_cache, BlockNumber segnum);
static void SetVersionIndexToCacheType1(dsa_pointer ptr_version_index_set, 
                                        uint32_t off_in_seg, 
                                        uint16_t len_of_rec,
                                        VersionIndexCache vi_cache, 
                                        Relation rel, BlockNumber seg_idx);
#endif
static int VersionIndexSetType1(Relation rel, BlockNumber blocknum, 
                                uint16_t lp_off, uint16_t lp_len, uint8_t hoff);
static int VersionIndexFlushForCheckPoint(int max_rel_idx);
static int VersionIndexFlushPerSegmentType1(Relation rel, BlockNumber segnum, 
                                            char * version_index_arr,
                                            completed_flag * completed_flag_arr,
                                            pg_atomic_uint64* p_num_cached_elem,
                                            uint32_t begin_idx, 
                                            uint32_t end_idx,
                                            uint32_t file_begin_idx,
                                            uint32_t num_of_flushed_vi);
#ifdef SMARTSSD_KT
static int VersionIndexFlushPerRelationType1(Relation rel, 
                                             VersionIndexCache vi_cache);
static int VersionIndexWaitFlushingOfRelation(pg_atomic_uint32* flushing_flag);
static int VersionIndexWakeupOtherFlushing(pg_atomic_uint32* flushing_flag);
static bool VersionIndexTryWaitFlushingOfRelation(pg_atomic_uint32* flushing_flag);
static int VersionIndexFlushType1(Relation rel);
#endif //SMARTSSD_KT

/*
 * VersionIndexShmemSize()
 *
 * Compute the size of shared memory of in-memory version index cache.
 */
Size
VersionIndexShmemSize(void)
{
  Size size = 0;

  // get size of version index cache data
  size = add_size(size, sizeof(VersionIndexCacheData) * MAX_NUM_RELATION);

  return size;
}

/*
 * VersionIndexInit()
 *
 * Initialize version index cache in shared memory.
 */
void
VersionIndexInit(void)
{
  bool found;

  // Initialize version index cache
  in_memory_version_index_cache = 
    (VersionIndexCache)ShmemInitStruct("Version Index Cache Descriptor",
      MAX_NUM_RELATION * sizeof(VersionIndexCacheData), &found);
}

/* VersionIndexInitDsa
 *
 * Initialize a dsa area for version index.
 * This function should only be called after the initization of 
 * the dynamic shared memory facilities.
 */
void
VersionIndexAttachDsa(void)
{
  /* Just get dsa area for NDP module */
  if (dsa_version_index == NULL)
  {
    dsa_version_index = dsa_attach(pg_atomic_read_u32(
                        &ProcGlobal->version_index_dsa_handle));
    dsa_pin_mapping(dsa_version_index);
  }
}

void
VersionIndexDetachDsa(void)
{
  if (dsa_version_index == NULL)
    return;
  dsa_detach(dsa_version_index);
  dsa_version_index = NULL;
}

/*
 * FindUnusedRelVersionIndex
 *
 * Find the index of version index cache slot mapped with relation
 * This function should be called with acquring alloc lock exclusively
 */
static int 
FindUnusedRelVersionIndex(RelFileNode rnode)
{
  int idx;

  Assert(in_memory_version_index_cache);

  for (idx = 0 ; idx < MAX_NUM_RELATION ; idx++)
  {
    // Find unused version index cache slot
    if (!in_memory_version_index_cache[idx].used)
    {
      return idx;
    }
    
    // Already version index cache slot mapped with relation exists 
    if (RelEquals(rnode, in_memory_version_index_cache[idx].rnode))
    {
      return -1;
    }
  }
  //Slots are full
  return idx;
}

/*
 * FindRelVersionIndex
 *
 * Find the index of version index cache slot mapped with relation
 * If there is no cached version index mapped with rnode, allocate new one
 */
static int 
FindRelVersionIndex(Relation rel)
{
  int idx;
  LWLock *alloc_version_index_lock;
  RelFileNode rnode;

  Assert(in_memory_version_index_cache);

  rnode = rel->rd_node;

  // Almost works're done in here!
  for (idx = 0 ; idx < MAX_NUM_RELATION ; idx++)
  {
    // Find version index cache slot mapped with relation
    if (in_memory_version_index_cache[idx].used && 
        RelEquals(rnode, in_memory_version_index_cache[idx].rnode))
    {
      return idx;
    }
    // Cannot find version index cache slot
    if (!in_memory_version_index_cache[idx].used) 
    {
      break;
    }
  }

  // Seldom case but correnctness shoule be observed

  // Cannot find version index cache slot
  // We have to allocate version index cache slot for relation
  alloc_version_index_lock = VIAllocLock();
  
  // Writing is needed so exclusive lock is acquired
  LWLockAcquire(alloc_version_index_lock, LW_EXCLUSIVE);
  
  if ((idx = FindUnusedRelVersionIndex(rnode)) == -1)
  {
    // Already version index cache slot is allocated
    // other processes allocated it first. There's nothing to do
    // Release lock and find and return idx of version index cache slot
    LWLockRelease(alloc_version_index_lock);
    return FindRelVersionIndex(rel);
  }   

  if (idx == MAX_NUM_RELATION)
  {
    LWLockRelease(alloc_version_index_lock);

    // Slots are full.
    // It's just PoC. If slots are full, then assert
    // TODO: Extend slot
    Assert(1);
    return -1;
  }
  
  // Set relation id and used flag
  in_memory_version_index_cache[idx].rnode= rnode;
  

  // Initialize version index cache resources
  in_memory_version_index_cache[idx].used = 1;
  pg_atomic_write_u32(&in_memory_version_index_cache[idx].flushing, 0);
  in_memory_version_index_cache[idx].type = rel->rd_vi_type;
  in_memory_version_index_cache[idx].lowest_segment_id = 0;
  in_memory_version_index_cache[idx].header_size = 0;

  memset(in_memory_version_index_cache[idx].version_index_set_per_segment, 0,
         sizeof(NDIRECTSEGPTR + 1) * sizeof(dsa_pointer));

  elog(INFO, "Allocation in version index cache [%d, %d]", idx, rnode.relNode);

  LWLockRelease(alloc_version_index_lock);

  return idx;
}

#ifdef SMARTSSD_KT
/*
 * GetDsaAreaForVersionIndexWithAlloc -- get dsa pointere used in version 
 *                                        index cache if not allocated,
 *                                        allocate one.
 */
static dsa_pointer 
GetDsaAreaForVersionIndexWithAlloc(pg_atomic_uint64* dst, dsa_pointer cmp_value, 
                                   size_t allocation_size)
{
  dsa_pointer ptr_dsa_area;

  ptr_dsa_area = cmp_value;

  // check if the address of dst is allocated.
  if (pg_atomic_compare_exchange_u64(dst, &ptr_dsa_area, UINT64_MAX))
  {
    // Case a) If the address of dst is not allocated.
    // This process is selected to create dsm for version index array 

    // create dsm for dst
    ptr_dsa_area = dsa_allocate_extended(dsa_version_index, allocation_size, 
                                         DSA_ALLOC_ZERO|DSA_ALLOC_HUGE);

    // Check if a dsa_pointer value is valid. 
    Assert(DsaPointerIsValid(ptr_dsa_area));

    pg_memory_barrier();

    // set dsa pointer to dst
    pg_atomic_write_u64(dst, ptr_dsa_area);
  }

  while (pg_atomic_read_u64(dst) == UINT64_MAX)
  {
    // Case b) version index for segment is not allocated not yet, and
    // another process is creating an dsm for version index array.
    // so just wait it to finish.
    pg_usleep(1);
  }

  return pg_atomic_read_u64(dst);
}

/*
 * GetVersionIndexSetPerSegmentPtrType1() -- get dsa pointer of version index 
 *                                           arr of segment
 */
static dsa_pointer
GetVersionIndexSetPerSegmentPtrType1(Relation rel,
                                     VersionIndexCache vi_cache_mapped_with_rel,
                                     BlockNumber segnum)
{
  VersionIndexIndirectArrPerSegment vi_set_per_segment;
  BlockNumber num_in_indirection_array;
  
  // Now, we find version index arr mapped with segnum.

  // if segnum is smaller than NDIRECTOFSETMENG.
  if (segnum < NDIRECTSEGPTR)
  {
    // get dsa pointer of version index array 
    return GetDsaAreaForVersionIndexWithAlloc(
      &(vi_cache_mapped_with_rel->version_index_set_per_segment[segnum]), 0, 
      sizeof(VersionIndexArrPerSegmentData));
  }

  // if segnum is smaller than NINDIRECTSEGPTR and larger than  
  // NDIRECTSEGPTR
  if (segnum < NINDIRECTSEGPTR)
  {
    vi_set_per_segment = dsa_get_address(dsa_version_index,
           GetDsaAreaForVersionIndexWithAlloc(
           &(vi_cache_mapped_with_rel->
           version_index_set_per_segment[NDIRECTSEGPTR]), 
           0, sizeof(VersionIndexIndirectArrPerSegmentData)));
    // Now, We need to search indirection array

    // get the location in indirection array
    num_in_indirection_array = segnum - NDIRECTSEGPTR;

    return GetDsaAreaForVersionIndexWithAlloc(
      &(vi_set_per_segment->
      version_index_set_per_segment[num_in_indirection_array]), 0, 
      sizeof(VersionIndexArrPerSegmentData));
  }
  // This is just PoC code yet. If number of segnemt excesses 
  // NINDIRECTSEGPTR(About 2TB), then assert.
  // TODO: Extend the supportable number of segment
  Assert(1);
  return 0;
}

/*
 * SetVersionIndexToCacheType1 -- set version index to in-memory cache
 */
static void 
SetVersionIndexToCacheType1(dsa_pointer ptr_version_index_set, 
                            uint32_t off_in_seg, uint16_t len_of_rec,
                            VersionIndexCache vi_cache, Relation rel,
                            BlockNumber seg_idx)
{
  VersionIndexArrPerSegment vi_array;

  uint64_t version_index_in_memory_idx;
  uint32_t idx_in_vi;
  char * set_ptr;

  // get version index set per segment
  vi_array = dsa_get_address(dsa_version_index, ptr_version_index_set);

  // get the location of version index
  version_index_in_memory_idx = pg_atomic_fetch_add_u64(&vi_array->num_of_elem, 
                                                        1);

  // After getting the location of version index, there is a probability 
  // version index cache is filled with. 
  // For these, re-check cache, and if space is not enough
  // flush it.
  while(version_index_in_memory_idx -
        pg_atomic_read_u64(&vi_array->num_of_flushed_elem) >= MAXNUMCACHEDVI);

  pg_memory_barrier();

  // get the idx in version index;
  idx_in_vi = version_index_in_memory_idx % MAXNUMCACHEDVI;

  /*****  Finally, we set version_index_pointer:*****/
  // Firstly, get in-memory address
  set_ptr = vi_array->version_index_arr + idx_in_vi * SIZEOFVITYPE1;
  
  // Set recodd offset in segment
  *((uint32*) set_ptr) = off_in_seg;
  set_ptr += 4;
  // Set length of record
  *((uint16*) set_ptr) = len_of_rec;

  pg_memory_barrier();

  // TODO: Can change this bitmap with atomic OR operations
  vi_array->completed_flag_arr[idx_in_vi] = NDP_TRUE;

  pg_memory_barrier();

  // increase the number of written version index in arr
   pg_atomic_fetch_add_u64(
    (pg_atomic_uint64*)(&vi_array->num_of_cached_elem), 1);
}
#endif //SMARTSSD_KT

/*
 * SetVersionIndexType1() -- set type 1 version index per segment
 *
 *    This is to be used for set new version index for table type 1.
 */
static int
VersionIndexSetType1(Relation rel, BlockNumber blocknum, uint16_t lp_off, 
                     uint16_t lp_len, uint8_t hoff)
{
#ifdef SMARTSSD_KT
  BlockNumber segnum;
  BlockNumber pagenum_in_seg;
  dsa_pointer ptr_version_index_set;
  uint32 off_in_seg;

  // Get version index cache of relation
  VersionIndexCache vicache = 
    &in_memory_version_index_cache[FindRelVersionIndex(rel)];

  // set size of header if its value is 0 
  if (!vicache->header_size)
    vicache->header_size = hoff;
  
  // get segment number and page num in segment
  pagenum_in_seg = smgrgetsegnumandrelativepagenum(rel->rd_smgr, MAIN_FORKNUM,
                                                   blocknum, &segnum);
  // get version index set set of segment
  ptr_version_index_set = GetVersionIndexSetPerSegmentPtrType1(rel, vicache, 
                                                               segnum);
  // get relative location in segment
  off_in_seg = BLCKSZ * pagenum_in_seg + lp_off;
  
  // Set version index to in-memory cache
  SetVersionIndexToCacheType1(ptr_version_index_set, off_in_seg, lp_len, 
                              vicache, rel, segnum);
#endif 
  return 0;
}


/*
 * VersionIndexSet -- call version index setting function per table type
 */
int
VersionIndexSet(Relation rel, BlockNumber blocknum, uint16_t lp_off, 
                uint16_t lp_len, uint8_t hoff)
{
	int len = 0;
  // followed by version index type, call version index setting function.
  switch (len)
  {
    case VI_TYPE1:
      // call type1 function. it just needs cachaing on-disk pointer.
      VersionIndexSetType1(rel, blocknum, lp_off, lp_len, hoff);
      break;
    case VI_TYPE2:
      //TODO:
      break;
    case VI_TYPE3:
      //TODO:
      break;
    default:
      // unknown version! assert.
      Assert(1);
  }
  return 0;
}

/*
 * VersionIndexFlushPerSegmentType1() -- Flush segment's version index
 *  TODO: Version index does't have to write sequentailly.
 * So, I think there are optimization points with this architecture 
 */
static int
VersionIndexFlushPerSegmentType1(Relation rel, BlockNumber segnum, 
                                 char * version_index_arr,
                                 completed_flag * completed_flag_arr,
                                 pg_atomic_uint64 *p_num_of_cached_elem,
                                 uint32_t begin_idx, 
                                 uint32_t end_idx,
                                 uint32_t file_begin_idx,
                                 uint32_t num_of_flushed_elem)
{
  int idx;
  
  /*
   *  version index write cache is circular queue
   *  .....O...........X.....
   *  flush from 'O' to 'X'
   */
  if (begin_idx < end_idx)
  {
    // For, flushing sequential area, Checking if area to write is filled with 
    // Completely
    for (idx = begin_idx ; idx < end_idx ; idx++)
    {
      // If not, spin wait
      while(!completed_flag_arr[idx])
      {
        pg_usleep(1); // sleep 1 micro sec
      }
    }

    pg_memory_barrier();

#if 0
    // After ensuring there are no hole in the area to write,
    // We can write it with one write call.
    smgrwritevi(rel->rd_smgr, MAIN_FORKNUM, segnum,
               version_index_arr + begin_idx * SIZEOFVITYPE1,
               SIZEOFVITYPE1 * (end_idx - begin_idx), 
               SIZEOFVICACHE * file_begin_idx + SIZEOFVITYPE1 * begin_idx + 
               VISTARTPTR);

    smgrwritevi(rel->rd_smgr, MAIN_FORKNUM, segnum,
                (char*)(&num_of_flushed_elem), SIZENUMFLUSHEDVI, 
                OFFNUMFLUSHEDVI);
#endif 
  
    // After writing, write down completed flag arr to notice these area are 
    // free
    memset(completed_flag_arr + begin_idx, NDP_FALSE,
           sizeof(completed_flag) * (end_idx - begin_idx));

    // sub cachec elem num
    pg_atomic_fetch_sub_u64(p_num_of_cached_elem, end_idx - begin_idx);

    return end_idx - begin_idx;
  }
  /*
   *  version index write cache
   *  .....X...........O.....
   *  flush from 'O' to 'X'
   */
  else
  {
    // For, flushing sequential area, Checking if area to write is filled with 
    // completely
    for (idx = begin_idx ; idx < MAXNUMCACHEDVI ; idx++)
    {
      while(!completed_flag_arr[idx])
      {
        pg_usleep(1); // sleep 1 micro sec
      }
    }

    for (idx = 0 ; idx < end_idx ; idx++)
    {
      while(!completed_flag_arr[idx])
      {
        pg_usleep(1); // sleep 1 micro sec
      }
    }

    pg_memory_barrier();

#if 0
    // After ensuring there are no hole in the area to write,
    // We can write it with one write call.
    smgrwritevi(rel->rd_smgr, MAIN_FORKNUM, segnum, 
                version_index_arr + begin_idx * SIZEOFVITYPE1,
                SIZEOFVITYPE1 * (MAXNUMCACHEDVI - begin_idx), 
                SIZEOFVICACHE * file_begin_idx + SIZEOFVITYPE1 * begin_idx +
                VISTARTPTR);

    smgrwritevi(rel->rd_smgr, MAIN_FORKNUM, segnum, version_index_arr,
                SIZEOFVITYPE1 * end_idx, SIZEOFVICACHE * (file_begin_idx + 1) +
                VISTARTPTR);

    smgrwritevi(rel->rd_smgr, MAIN_FORKNUM, segnum,
                (char*)(&num_of_flushed_elem), SIZENUMFLUSHEDVI, 
                OFFNUMFLUSHEDVI);
#endif 

    // After writing, write down completed flag arr to notice these area are 
    // free
    memset(completed_flag_arr + begin_idx, NDP_FALSE,
           sizeof(completed_flag) * (MAXNUMCACHEDVI - begin_idx));

    memset(completed_flag_arr, NDP_FALSE, sizeof(completed_flag) * end_idx);

    pg_memory_barrier();

    // sub cachec elem num
    pg_atomic_fetch_sub_u64(p_num_of_cached_elem, SIZEOFVICACHE - begin_idx);

    pg_atomic_fetch_sub_u64(p_num_of_cached_elem, end_idx);

    return end_idx + MAXNUMCACHEDVI - begin_idx;
  }
}

/*
 * VersionIndexFlushPerSegofRelType1() -- Flush segment's version index 
 */
int 
VersionIndexFlushPerSegofRelType1(Relation rel, VersionIndexCache vi_cache, 
                                  VersionIndexArrPerSegment vi_array, 
                                  BlockNumber seg_idx)
{
  uint64_t cur_num_of_flushed_elem;
  uint64_t cur_num_of_elem;
  uint64_t internal_idx_begin_arr, internal_idx_end_arr;
  uint64_t file_begin_loc; 

  // Check if this relation exists
  if (!smgrexists(rel->rd_smgr, MAIN_FORKNUM))
  {
    // if relation doesn't exists, the return
    //  smgrclose(rel->rd_smgr);
    return 0;
  }

  // Flush version index of all segments in table
  // Version index array set is similar to circular queue.
  // From the last point of previous flush to the current last point of 
  // element, flush every version index with compressed form
  cur_num_of_flushed_elem =
        pg_atomic_read_u64(&vi_array->num_of_flushed_elem);

  pg_memory_barrier();

  cur_num_of_elem = pg_atomic_read_u64(&vi_array->num_of_elem);
  
  if (cur_num_of_elem < cur_num_of_flushed_elem)
  {
    elog(ERROR, "num of flushed elem is larger than total number of element %lu %lu",
          cur_num_of_flushed_elem, cur_num_of_elem); 
    Assert(cur_num_of_flushed_elem < cur_num_of_elem);
    return -1;
  }

  // There is no version index generated after previous flush
  if (cur_num_of_flushed_elem == cur_num_of_elem)
  {
    return 0;
  }
    
  // Get actual number of cached vi
  if (cur_num_of_elem - cur_num_of_flushed_elem > MAXNUMCACHEDVI)
    cur_num_of_elem = cur_num_of_flushed_elem + MAXNUMCACHEDVI;

  // Get the segment number of flush begin point
  file_begin_loc = cur_num_of_flushed_elem / MAXNUMCACHEDVI;

  // get start,end point of flush
  internal_idx_begin_arr = cur_num_of_flushed_elem % MAXNUMCACHEDVI;
  internal_idx_end_arr = cur_num_of_elem % MAXNUMCACHEDVI;
  
  if (!internal_idx_end_arr)
  {
    internal_idx_end_arr = MAXNUMCACHEDVI;
  }
    
  // Flush version index of segment
  VersionIndexFlushPerSegmentType1(rel, seg_idx + vi_cache->lowest_segment_id, 
                                   vi_array->version_index_arr,
                                   vi_array->completed_flag_arr,
                                   &vi_array->num_of_cached_elem,
                                   internal_idx_begin_arr, 
                                   internal_idx_end_arr,
                                   file_begin_loc,
                                   cur_num_of_elem);

  pg_memory_barrier();
  // Increase the number of flushed version indexes
  pg_atomic_add_fetch_u64(&vi_array->num_of_flushed_elem, cur_num_of_elem - 
                                                    cur_num_of_flushed_elem);
  return 0;
}


#ifdef SMARTSSD_KT
/*
 * VersionIndexFlushPerRelationType1() -- Flush relation's version index 
 */
static int 
VersionIndexFlushPerRelationType1(Relation rel, VersionIndexCache vi_cache)
{
  VersionIndexIndirectArrPerSegment vi_indirect_arr; 
  VersionIndexArrPerSegment vi_array;

  dsa_pointer target_vi_arr_ptr;
  uint64_t cur_num_of_flushed_elem;
  uint64_t cur_num_of_elem;
  uint64_t internal_idx_begin_arr, internal_idx_end_arr;
  uint64_t file_begin_loc; 
  int seg_idx; 
  int max_num_segment;

  // Check if this relation exists
  if (!smgrexists(rel->rd_smgr, MAIN_FORKNUM))
  {
    // if relation doesn't exists, the return
    //  smgrclose(rel->rd_smgr);
    return 0;
  }

  // Get the current maximum number of segment
  max_num_segment = smgrnblocks(rel->rd_smgr, MAIN_FORKNUM) / RELSEG_SIZE + 1;

  // If the number of segment is larger than NDIRECTSEGPTR,
  // we should refer to indirection array to flush version index of segment
  if (max_num_segment > NDIRECTSEGPTR)
  {
    // get indirection array
    vi_indirect_arr = dsa_get_address(dsa_version_index, 
      pg_atomic_read_u64(
        &vi_cache->version_index_set_per_segment[NDIRECTSEGPTR]));
  }
  
  // Flush version index of all segments in table
  for (seg_idx = 0 ; seg_idx < max_num_segment ; seg_idx++)
  {
    target_vi_arr_ptr = 0;
    // Pointing version index array directly
    if (seg_idx < NDIRECTSEGPTR)
    {
    // Get version index array set of segment
      target_vi_arr_ptr = 
        pg_atomic_read_u64(&vi_cache->version_index_set_per_segment[seg_idx]);
    }
    // Pointing version index array indirectly
    else if (seg_idx < NINDIRECTSEGPTR)
    {
        // Get version index array set of segment from indirect array
      target_vi_arr_ptr = 
          pg_atomic_read_u64(&vi_indirect_arr->
          version_index_set_per_segment[seg_idx - NDIRECTSEGPTR]);
    }
  
    if (target_vi_arr_ptr == 0)
    {
      continue;
    }

    // Get version index array set of segment
    vi_array = dsa_get_address(dsa_version_index, target_vi_arr_ptr);

    // Version index array set is similar to circular queue.
    // From the last point of previous flush to the current last point of 
    // element, flush every version index with compressed form
    cur_num_of_flushed_elem =
        pg_atomic_read_u64(&vi_array->num_of_flushed_elem);

    pg_memory_barrier();

    cur_num_of_elem = pg_atomic_read_u64(&vi_array->num_of_elem);
  
    if (cur_num_of_elem < cur_num_of_flushed_elem)
    {
      elog(ERROR, "num of flushed elem  is larger than total number of element %lu %lu",
           cur_num_of_flushed_elem, cur_num_of_elem); 
      Assert(cur_num_of_flushed_elem < cur_num_of_elem);
    }

    // There is no version index generated after previous flush
    if (cur_num_of_flushed_elem == cur_num_of_elem)
    {
      continue;
    }
    
    // Get actual number of cached vi
    if (cur_num_of_elem - cur_num_of_flushed_elem > MAXNUMCACHEDVI)
      cur_num_of_elem = cur_num_of_flushed_elem + MAXNUMCACHEDVI;

    // Get the segment number of flush begin point
    file_begin_loc = cur_num_of_flushed_elem / MAXNUMCACHEDVI;

    // get start,end point of flush
    internal_idx_begin_arr = cur_num_of_flushed_elem % MAXNUMCACHEDVI;
    internal_idx_end_arr = cur_num_of_elem % MAXNUMCACHEDVI;
    
    if (!internal_idx_end_arr)
    {
      internal_idx_end_arr = MAXNUMCACHEDVI;
    }
      
    // Flush version index of segment
    VersionIndexFlushPerSegmentType1(rel, seg_idx + vi_cache->lowest_segment_id, 
                                     vi_array->version_index_arr,
                                     vi_array->completed_flag_arr,
                                     &vi_array->num_of_cached_elem,
                                     internal_idx_begin_arr, 
                                     internal_idx_end_arr,
                                     file_begin_loc,
                                     cur_num_of_elem);

    pg_memory_barrier();
    // Increase the number of flushed version indexes
    pg_atomic_add_fetch_u64(&vi_array->num_of_flushed_elem, cur_num_of_elem - 
                                                      cur_num_of_flushed_elem);
  }
  return 0;
}

/*
 * VersionIndexWaitFlushingOfRelation() -- Wait to finish flushing
 * TODO: It's just spinwait. If needed, it have to be improved
 */
static int
VersionIndexWaitFlushingOfRelation(pg_atomic_uint32* flushing_flag)
{
  uint32_t expected = 0; 

  // turn on flushing flag of version index cache of realation
  while (!pg_atomic_compare_exchange_u32(flushing_flag, &expected, UINT32_MAX));

  return 0;
}

/*
 * VersionIndexWakeupOtherFlushing() -- By turning off flushing flag, wake up
 * other flushsing if they are existed
 * TODO: It's just spinwait. If needed, it have to be improved
 */
static int
VersionIndexWakeupOtherFlushing(pg_atomic_uint32* flushing_flag)
{
  uint32_t expected = UINT32_MAX;

  // turn off flushing flag of version index cache of realation
  while (!pg_atomic_compare_exchange_u32(flushing_flag, &expected, 0));
  
  return 0;
}

/*
 * VersionIndexTryWaitFlushingOfRelation() -- Try waiting to finish flushing
 */
static bool
VersionIndexTryWaitFlushingOfRelation(pg_atomic_uint32* flushing_flag)
{
  uint32_t expected = 0;

  // turn on flushing flag of version index cache of realation
  return pg_atomic_compare_exchange_u32(flushing_flag, &expected, UINT32_MAX);
}

/*
 * VersionIndexFlushType1() -- Flush version index of type 1 relation
 */ 
static int
VersionIndexFlushType1(Relation rel)
{
  int rel_idx; 
  VersionIndexCache vi_cache_mapped_with_rel;

  // Get the index of version index set mapped with relation
  rel_idx = FindRelVersionIndex(rel);

  // Get the version index cache mapped with relation
  vi_cache_mapped_with_rel = &in_memory_version_index_cache[rel_idx];
  
  // Wait to finish another flush
  VersionIndexWaitFlushingOfRelation(&vi_cache_mapped_with_rel->flushing);

  // Flush relation's version index per segment
  VersionIndexFlushPerRelationType1(rel, vi_cache_mapped_with_rel);
  
  // Wake up other flushing
  VersionIndexWakeupOtherFlushing(&vi_cache_mapped_with_rel->flushing);

  return 0;
}
#endif //SMARTSSD_KT

/*
 * VersionIndexFlush -- call version index flush function per table type
 */
int
VersionIndexFlush(Relation rel)
{
#ifdef SMARTSSD_KT
  // followed by version index type, call version index setting function.
  switch (RelationCacheGetVIType(rel))
  {
    case VI_TYPE1:
      // For, type1.
      VersionIndexFlushType1(rel);
      break;
    case VI_TYPE2:
      //TODO:
      break;
    case VI_TYPE3:
      //TODO:
      break;
    default:
      // unknown version! assert.
      Assert(1);
  }
#endif //SMARTSSD_KT
  return 0;
}

/*
 * VersionIndexFlushForCheckPoint - 
 */
static int
VersionIndexFlushForCheckPoint(int max_rel_idx)
{
#ifdef SMARTSSD_KT
  int rel_idx;
  int arr_idx;
  int begin_idx, end_idx;
  int file_begin_idx;
  uint64 cur_num_of_elem;
  uint64 cur_num_of_flushed_elem;
  uint32 seg_idx, max_seg_no, flushed_elem;

  VersionIndexCache vi_cache;
  VersionIndexIndirectArrPerSegment indirect_vi_array = NULL;
  VersionIndexArrPerSegment vi_array;
  dsa_pointer vi_arr_ptr; 
  SMgrRelation reln;

  for (rel_idx = 0 ; rel_idx < max_rel_idx ; rel_idx++)
  {
    // TODO: currently, this code is executed for type1 we need to make this
    // all of types
    vi_cache = &in_memory_version_index_cache[rel_idx];

    // Try waiting to finish another flush
    // If not succeeding, it means other worker are flushing this relations.
    // Checkpointer and log writer doesn't need to flush all of version index.
    // So, If not succeeding, try to flush version indexes of other relations.
    if (!VersionIndexTryWaitFlushingOfRelation(&vi_cache->flushing))
    {
      continue;
    }

    // VersionIndexWaitFlushingOfRelation(&vi_cache->flushing);

    if (vi_cache->type == VI_TYPE1)
    {
      // get SmgrRelation for checkpointer for file I/O
      reln = smgropen(vi_cache->rnode, InvalidBackendId); 

      // Check if this relation exists
      if (!smgrexists(reln, MAIN_FORKNUM))
      {
        smgrclose(reln);
        VersionIndexWakeupOtherFlushing(&vi_cache->flushing);
        continue;
      }

      // get max number of segment
      max_seg_no = smgrnblocks(reln, MAIN_FORKNUM) / RELSEG_SIZE + 1;

      if (max_seg_no == 0)
      {
        VersionIndexWakeupOtherFlushing(&vi_cache->flushing);
        continue;
      }

      // if max seg number is larger than directly pointer version index number 
      if (max_seg_no >= NDIRECTSEGPTR && pg_atomic_read_u64(
          &vi_cache->version_index_set_per_segment[NDIRECTSEGPTR]))
      {
        //  Get indirect pointer array
        indirect_vi_array = 
          dsa_get_address(dsa_version_index, 
                          pg_atomic_read_u64(&vi_cache->
                          version_index_set_per_segment[NDIRECTSEGPTR]));
      }

      // Flush version index of segment
      for (seg_idx = 0; seg_idx < max_seg_no ; seg_idx++)
      {
        if (seg_idx < NDIRECTSEGPTR)
        {
          // get version index pointer
          vi_arr_ptr = 
          pg_atomic_read_u64(&vi_cache->version_index_set_per_segment[seg_idx]);
        } 
        else if (seg_idx < NINDIRECTSEGPTR)
        {
          // get version index
          vi_arr_ptr = pg_atomic_read_u64(&indirect_vi_array->
                        version_index_set_per_segment[seg_idx - NDIRECTSEGPTR]);
        }

        if (!vi_arr_ptr)
        {
          continue;
        }
    
        vi_array = dsa_get_address(dsa_version_index, vi_arr_ptr);
      
        // flush from flushed num to elem num    
        cur_num_of_flushed_elem = 
                          pg_atomic_read_u64(&vi_array->num_of_flushed_elem);

        pg_memory_barrier();      

        cur_num_of_elem = pg_atomic_read_u64(&vi_array->num_of_elem);

        if (cur_num_of_elem < cur_num_of_flushed_elem)
        {
          elog(ERROR, 
              "flushed vi number is larger than total vi number in checkpoint %lu %lu",
              cur_num_of_flushed_elem, cur_num_of_elem); 
          Assert(cur_num_of_flushed_elem < cur_num_of_elem);
        }

        if (cur_num_of_flushed_elem == cur_num_of_elem)
        {
          continue;
        }

        // Get actual number of cached vi
        if (cur_num_of_elem - cur_num_of_flushed_elem > MAXNUMCACHEDVI)
          cur_num_of_elem = cur_num_of_flushed_elem + MAXNUMCACHEDVI;

        // get begin, end point in version index cache
        begin_idx = cur_num_of_flushed_elem % MAXNUMCACHEDVI;
        end_idx = cur_num_of_elem % MAXNUMCACHEDVI;

        // get start point in file
        file_begin_idx = cur_num_of_flushed_elem / MAXNUMCACHEDVI;

        if (!end_idx)
        {
          end_idx = MAXNUMCACHEDVI;
        }

        /*
         *  version index write cache is circular queue
         *  .....O...........X.....
         *  flush from 'O' to 'X'
         */
        if (begin_idx < end_idx)
        {
          // For, flushing sequential area, Checking if area to write is filled 
          // with completely
          for (arr_idx = begin_idx ; arr_idx < end_idx ; arr_idx++)
          {
            // If not, spin wait
            while(!vi_array->completed_flag_arr[arr_idx])
            {
              pg_usleep(1); // sleep 1 micro sec
            }
          }
  
          pg_memory_barrier();
      
#if 0
          // After ensuring there are no hole in the area to write,
          // We can write it with one write call.
          smgrwritevi(reln, MAIN_FORKNUM, seg_idx + vi_cache->lowest_segment_id,
                      vi_array->version_index_arr + begin_idx * SIZEOFVITYPE1,
                      SIZEOFVITYPE1 * (end_idx - begin_idx),
                      SIZEOFVICACHE * file_begin_idx + SIZEOFVITYPE1 * begin_idx
                      + VISTARTPTR);
          // After writing, write down completed flag arr to notice these area 
          // are free
          memset(vi_array->completed_flag_arr + begin_idx, NDP_FALSE,
                 sizeof(completed_flag) * (end_idx - begin_idx));

          flushed_elem = cur_num_of_elem;

          smgrwritevi(reln, MAIN_FORKNUM, seg_idx + vi_cache->lowest_segment_id,
                      (char*)(&flushed_elem), SIZENUMFLUSHEDVI, 
                      OFFNUMFLUSHEDVI);
#endif

          // sub cached elem num
          pg_atomic_fetch_sub_u64(
                             (pg_atomic_uint64*)(&vi_array->num_of_cached_elem),
                              end_idx - begin_idx);
        }
        /*
         *  version index write cache
         *  .....X...........O.....
         *  flush from 'O' to 'X'
         */
        else
        {
          // For, flushing sequential area, Checking if area to write is filled 
          // with completely
          for (arr_idx = begin_idx ; arr_idx < MAXNUMCACHEDVI ; arr_idx++)
          {
            while(!vi_array->completed_flag_arr[arr_idx])
            {
              pg_usleep(1); // sleep 1 micro sec
            }
          }

          for (arr_idx = 0 ; arr_idx < end_idx ; arr_idx++)
          {
            while(!vi_array->completed_flag_arr[arr_idx])
            {
              pg_usleep(1); // sleep 1 micro sec
            }
          }

          pg_memory_barrier();

#if 0
          // After ensuring there are no hole in the area to write,
          // We can write it with two write calls.
          smgrwritevi(reln, MAIN_FORKNUM, seg_idx + vi_cache->lowest_segment_id,
                      vi_array->version_index_arr + begin_idx * SIZEOFVITYPE1,
                      SIZEOFVITYPE1 * (MAXNUMCACHEDVI - begin_idx), 
                      SIZEOFVICACHE * file_begin_idx + SIZEOFVITYPE1 * begin_idx
                      + VISTARTPTR);

          smgrwritevi(reln, MAIN_FORKNUM, seg_idx + vi_cache->lowest_segment_id, 
                      vi_array->version_index_arr,
                      SIZEOFVITYPE1 * end_idx, 
                      SIZEOFVICACHE * (file_begin_idx + 1) + VISTARTPTR);
#endif

          // After writing, write down completed flag arr to notice these area 
          // are free
          memset(vi_array->completed_flag_arr + begin_idx, NDP_FALSE,
                 sizeof(completed_flag) * (MAXNUMCACHEDVI - begin_idx));

          memset(vi_array->completed_flag_arr, NDP_FALSE, 
                 sizeof(completed_flag) * end_idx);

          flushed_elem = cur_num_of_elem;

          pg_memory_barrier();

#if 0
          smgrwritevi(reln, MAIN_FORKNUM, seg_idx + vi_cache->lowest_segment_id,
                      (char*)(&flushed_elem), SIZENUMFLUSHEDVI, 
                      OFFNUMFLUSHEDVI);
#endif
          // Substract the number of cached element
          pg_atomic_fetch_sub_u64(
                               (pg_atomic_uint64*)(&vi_array->num_of_cached_elem), 
                               SIZEOFVICACHE - begin_idx);

          pg_atomic_fetch_sub_u64(
            (pg_atomic_uint64*)(&vi_array->num_of_cached_elem), end_idx);
        }

        pg_memory_barrier(); 

        // Add the number of flushed element

        pg_atomic_add_fetch_u64(&vi_array->num_of_flushed_elem, 
                                cur_num_of_elem - cur_num_of_flushed_elem);
      }
    } 
    else if (vi_cache->type == VI_TYPE2)
    {
    }
    else if (vi_cache->type == VI_TYPE3)
    {
    }
    
    // Wake up other flushing worker
    VersionIndexWakeupOtherFlushing(&vi_cache->flushing);
  }
#endif //SMARTSSD_KT

  return 0;
}

/*
 * CheckPointVersionIndex -- checkpointing version index cache data
 */
int
CheckPointVersionIndex(void)
{
  int max_rel_idx;

  LWLockAcquire(VIAllocLock(), LW_SHARED); 

  // get the maximum number of relation index
  for (max_rel_idx = 0; max_rel_idx < MAX_NUM_RELATION ; max_rel_idx++)
  {
    if (!in_memory_version_index_cache[max_rel_idx].used)
    {
      break;
    }
  }
  
  LWLockRelease(VIAllocLock());

  // flush all of version index cache per relation
  VersionIndexFlushForCheckPoint(max_rel_idx);

  return 0;
}

uint8_t GetHeaderSize(Relation rel)
{
  VersionIndexCache vicache = 
    &in_memory_version_index_cache[FindRelVersionIndex(rel)];

  Assert(vicache->header_size);

  return vicache->header_size;
}

#ifdef SMARTSSD_VI_DEBUG
int
PrintOffAndLen(Relation rel, uint32_t off, uint16_t len, uint32_t block_no)
{
  int i = 0;
  uint8_t offs[4];
  uint8_t lens[2];
  char file_name[256];
  int test_fd;
  int rel_idx = FindRelVersionIndex(rel);

  sprintf(file_name, "/opt/pm883-1/ktlee20/compare_set_%d_%d", rel_idx, 
          block_no / RELSEG_SIZE);

  if (access(file_name, F_OK) == -1)
  {
    test_fd = open(file_name, O_RDWR|O_CREAT|O_APPEND, 0644);
    write(test_fd, &i, 4);
  }
  else
  {
    test_fd = open(file_name, O_RDWR|O_CREAT|O_APPEND, 0644);
  }

  for (i = 0 ; i < 4 ; i++)
  {
    offs[i] = off % 256;
    write(test_fd, &offs[i], 1);
    off /= 256;
  }

  for (i = 0 ; i < 2 ; i++)
  {
    lens[i] = len % 256;
    write(test_fd, &lens[i], 1);
    len /= 256;
  }

  close(test_fd);

  return 0;
}

#endif

#endif // #ifdef SMARTSSD
