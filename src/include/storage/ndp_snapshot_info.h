/*-------------------------------------------------------------------------
 * ndp_snapshot_info.h
 * 
 * Interface of the snapshot track module for ndp
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *    src/include/storage/ndp_snapshot_info.h
 *-------------------------------------------------------------------------
 */

#ifndef NDP_SNAPSHOT_INFO_H
#define NDP_SNAPSHOT_INFO_H

#include "c.h"

#include "postgres.h"
#include "utils/snapshot.h"

typedef struct {
  TransactionId xmin;
  TransactionId xmax;
  uint32         xcnt;
} SnapshotContainer;

int GetSnapshotContainer(void* snap_con, Snapshot snapshot, TransactionId committed);

#endif
