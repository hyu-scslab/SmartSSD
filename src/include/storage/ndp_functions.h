#ifndef _NDP_FUNCTIONS_H_
#define _NDP_FUNCTIONS_H_

#ifdef __cplusplus

#ifndef SMARTSSD
#define SMARTSSD
#endif
#ifndef SMARTSSD_DEBUG
#define SMARTSSD_DEBUG
#endif

#include "NDPObject.hpp"
#include "FPGAManager.hpp"

#else

void *FPGAManager_GetInstance(char *xclbin_file_path, bool enable_p2p);

void *NDPObject_GetInstance(void *fm, void *ndpctl, char *oidPath);

bool Has_CurrentJob();

void Destruct_NDPObject(void *ndpo);

void Destruct_FPGAManager(void *fm);

int Get_Packets(void *ndpo, int rel_oid, char *packet, int blockNum);

int Get_TableIndex(void *ndpo, int rel_oid);

long long int Get_total_packet_size(void *ndpo, int table_index);

bool filter_Join_SSD_ooo(void *ndpo);

bool FlushVersionIndex(void *ndpo, int table_index, int seg_num, int *seg_fd, int *ifile_fd);

int DeviceIsFinish(void *ndpo);

#endif  // __cplusplus

#endif  // _NDP_FUNCTIONS_H_
