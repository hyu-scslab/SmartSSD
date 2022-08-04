/*-------------------------------------------------------------------------
 * ndp_table_info.h
 * 
 * Interfaces of the routines to manage table info for ndp.
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *    src/include/storage/ndp_table_info.h
 *-------------------------------------------------------------------------
 */

#if defined(SMARTSSD)

#ifndef NDP_TABLE_INFO_H
#define NDP_TABLE_INFO_H

#include "c.h"
#include "postgres.h"

#include "nodes/execnodes.h"
#include "utils/relcache.h"

typedef enum _NUM_OP_MODE_ {NDP_SCAN, NDP_JOIN} NUM_OP_MODE;

struct _NDPTableInfo_
{
	uint16 offset;
	uint16 length;
};

typedef struct _NDPTableInfo_ NDPTableInfo; 

uint16* GetAttrSizeAndOffset(Relation rel);

#endif
#endif
