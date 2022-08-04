#ifdef SMARTSSD
#ifndef __NDP_USER_CC_H__
#define __NDP_USER_CC_H__

typedef struct NDPLWSpinlock
{
  dsa_pointer successor; 
  pg_atomic_flag locked;
} NDPLWSpinlockData;

typedef struct NDPLWSpinlockData NDPLWSpinlock;

void NDPAttachDsaArea(pg_atomic_uint64 *p_dsa_area);
void NAPDetachDsaArea(pg_atomic_uint64 *p_dsa_area);
dsa_pointer NDPAcquireSpinlock(pg_atomic_uint64 *p_dsa_area,
                               pg_atomic_uint64 *last_lock_ptr);
void NDPReleaseSpinlock(pg_atomic_uint64 *p_dsa_area,
                        pg_atomic_uint64 *last_lock_ptr, 
                        dsa_pointer my_lock_ptr);
                   

#endif
#endif
