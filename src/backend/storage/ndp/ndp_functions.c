/*-------------------------------------------------------------------------
 *
 * ndp_functions.c
 *	  Version Index Manager Implementation
 *
 * NOTE: This branch is defined just to avoid compiler errors on machines
 * without SmartSSD. Test environments without SmartSSD may proceed with
 * everything else but NDP functions.
 *
 * src/backend/storage/ndp/ndp_functions.c
 *
 *-------------------------------------------------------------------------
 */

#ifndef SMARTSSD_EXISTS

#include "postgres.h"

#include "storage/ndp_functions.h"

void *
FPGAManager_GetInstance(char *xclbin_file_path, bool enable_p2p)
{
	return NULL;
}

void *
NDPObject_GetInstance(void *fm, void *ndpctl, char *oidPath)
{
	return NULL;
}
bool Has_CurrentJob()
{
    return 0;
}

int
Get_Packets(void *ndpo, int rel_oid, char *packet, int blockNum)
{
	return 0;
}

int
Get_TableIndex(void *ndpo, int rel_oid)
{
	return 0;
}

long long int
Get_total_packet_size(void *ndpo, int table_index)
{
	return 0;
}

bool
FlushVersionIndex(void *ndpo, int table_index, int seg_num, int *seg_fd, int *ifile_fd)
{
	return true;
}

int DeviceIsFinish(void *ndpo)
{
	return true;
}

#endif	/* SMARTSSD_EXISTS */
