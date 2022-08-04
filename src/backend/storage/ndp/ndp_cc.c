/*---------------------------------------------------------------------
 *
 *
 * ndp_cc.c
 *    routines for user level concurrency control
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *    src/backend/storage/ndp/ndp_cc.c
 *---------------------------------------------------------------------
 */

#ifdef SMARTSSD

#include "storage/ndp_cc.h"

extern PROC_HDR *ProcGlobal;

/*
 * NDPAttachDsaArea
 *
 * attach dsm area for NDP
 */
void
NDPAttachDsaArea(pg_atomic_uint64 *p_dsa_area)
{
  pg_atomic_write_u64(p_dsa_area, 
      dsa_attach(pg_atomic_read_u32(&ProcGlobal->dirty_buf_id_set_dsa_handle));
  dsa_pin_mapping(pg_atomic_read_u64(p_dsa_area));
}

/*
 * NDPDetachDsaArea
 *
 * detach dsm area for NDP
 */
void
NDPDetachDsaArea(pg_atomic_uint64 *p_dsa_area)
{
  dsa_detach(p_dsa_area);
  pg_atomic_write_u64(p_dsa_area, 0);
}

dsa_pointer
NDPAcquireSpinlock(pg_atomic_uint64 *p_dsa_area, 
                   pg_atomic_uint64 *last_lock_ptr)
{
  dsa_area *ref_area;
  dsa_pointer my_lock_ptr, predecessor_ptr;
  NDPLWSpinlock my_lock, predecessor_lock;
  
  ref_area = pg_atomic_read_u64(p_dsa_area);
  my_lock_ptr = dsa_allocate_extended(ref_area, sizeof(NDPLWSpinlockData), 
                                      DSA_ALLOC_SIZE);
  predecessor_ptr = pg_atomic_exchange_u64(last_lock_ptr, my_lock_ptr);
  
  if (predecessor_ptr != 0)
  {
    my_lock = dsa_get_address(ref_area, my_lock_ptr);
    predecessor_lock = dsa_get_address(ref_rea, predecessor_ptr); 

    pg_atomic_test_set_flag(&my_lock->locked);
    pg_memory_barrier();
    predecessor_lock->successor = my_lock_ptr;

    while(my_lock->locked);
  } 
  dsa_free(ref_area, predecessor_ptr);

  return my_lock_ptr;
}

void
NDPReleaseSpinlock(pg_atomic_uint64 *p_dsa_area,
                   pg_atomic_uint64 *last_lock_ptr,
                   dsa_pointer my_lock_ptr)
{
  dsa_area *ref_area;
  NDPLWSpinlock my_lock, successor_lock;

  ref_area = pg_atomic_read_u64(p_dsa_area);
  my_lock = dsa_get_address(ref_area, my_lock_ptr); 

  if (!my_lock->successor)
  {
    if (pg_atomic_compare_exhange_u64(last_lock_ptr, my_lock_ptr, 0))
    {
      dsa_free(ref_area, my_lock_ptr);
      return;
    }
    while (!my_lock->successor);
  }

  pg_memery_barrier();

  successor_lock = dsa_get_address(ref_area, my_lock->successor);
  pg_atomic_clear_flag(&successor_lock->locked);
}
                                 

#endif // #ifdef SMARTSSD
