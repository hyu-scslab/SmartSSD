/*---------------------------------------------------------------------
 *
 *
 * ndp_snapshot_info.c
 *    routines to managea dirty buf id sets for page flush per relation.
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *    src/backend/storage/ssd/ndp_snapshot_info.c
 *---------------------------------------------------------------------
 */

#include "storage/ndp_snapshot_info.h"
#include "utils/snapshot.h"

#include <stdlib.h>

/*
 * GetSnapshotContainer
 *
 * Get snapshot container from snapshot
 */
int
GetSnapshotContainer(void* snap_con, Snapshot snapshot, TransactionId committedXid)
{
	char* ptr = snap_con;
	*((TransactionId*) ptr) = snapshot->xmin;
	ptr += sizeof(TransactionId);

	*((TransactionId*) ptr) = snapshot->xmax;
	ptr += sizeof(TransactionId);

	*((CommandId*) ptr) = snapshot->curcid;
	ptr += sizeof(CommandId);

	*((uint64*) ptr) = snapshot->xcnt;
	ptr += sizeof(uint32);

	memcpy(ptr, snapshot->xip, snapshot->xcnt * sizeof(TransactionId));
	ptr += snapshot->xcnt * sizeof(TransactionId);

	/*
	*((TransactionId*) ptr) = committedXid;
	ptr += sizeof(TransactionId);
	*/

	return ptr - (char*) snap_con;
}
