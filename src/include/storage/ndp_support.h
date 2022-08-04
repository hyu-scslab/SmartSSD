/*-------------------------------------------------------------------------
 * ndp_support.h
 * 
 * Interface of the information tracking module for smart ssd used for multi 
 * table join optimization.
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *    src/include/storage/ndp_support.h
 *-------------------------------------------------------------------------
 */
#if defined(SMARTSSD) || defined(SMARTSSD_DBHF)

#ifndef NDP_SUPPORT_H
#define NDP_SUPPORT_H

#include "postgres.h"
#include "nodes/execnodes.h"

#ifndef MAX_NUM_RELATION
/* max number of relation */
#define MAX_NUM_RELATION 16
#endif

#ifdef WSUL_CTID_TEST
typedef struct __attribute__((__packed__)) CanonicalTupleData {
	unsigned offset: 30,
					 len: 10,
					 h_len: 9,
					 bits: 23;
} CanonicalTupleData;

typedef CanonicalTupleData* CanonicalTuple;
#endif //WSUL_CTID_TEST

typedef enum NDPFilterAttType {
	NDP_FILTER_STRING = 0,
	NDP_FILTER_2B,
	NDP_FILTER_4B,
	NDP_FILTER_8B,
} NDPFilterAttType;

typedef enum NDPFilterOpType {
	NDP_FILTER_EQ = 0,
	NDP_FILTER_GT = 1,
	NDP_FILTER_GTE = 2,
	NDP_FILTER_LT = 3,
	NDP_FILTER_LTE = 4,
	NDP_FILTER_NONE = 5
} NDPFilterOpType;

typedef enum NDPCommandType {
	InvalidCommand = -1,
	NDP_COMMAND_JOIN = 0,
} NDPCommandType;

typedef struct __attribute__((__packed__)) NDPFilter {
	uint8 attr_type;// 0: string, 1: 2B, 2: 4B, 3: 8B
	uint64 operand; // up-to 8B value
	uint8 op_type;  // one of NDPFilterOpType values
	uint8 prefix_len;// used for string match
	uint8 target_idx;// column index for a filter target
} NDPFilter;

typedef struct __attribute__((__packed__)) NDPFilterInfo {
	uint16 num_filter;
	NDPFilter filters[0];
} NDPFilterInfo;

typedef struct __attribute__((__packed__)) NDPColumnInfo {
	uint16 num_att;
	uint16 attlen[0];
	uint16 hash_key_index;
	uint16 hash_key_len;
} NDPColumnInfo;

typedef struct __attribute__((__packed__)) NDPRelationInfoData {
	Oid rel_id;
	uint32 num_segs;
	NDPColumnInfo column_info;
	NDPFilterInfo filter_info;
} NDPRelationInfoData;

typedef struct NDPRelationInfoData* NDPRelationInfo;

struct __attribute__((__packed__)) NDPCommandMessageData {
	NDPCommandType command_type;
	uint32 len;
	uint32 num_relations;
	NDPRelationInfoData relations[0];
} NDPCommandMessageData;

typedef struct NDPCommandMessageData* NDPCommandMessage;

typedef struct NDPRelationFileSegment {
	int data;
	int ctid;
} NDPRelationFileSegment;

typedef struct NDPRelationFileData {
	Oid id;
	uint32 num_segs;
	NDPRelationFileSegment seg[0];
} NDPRelationFileData;

typedef struct NDPRelationFileData* NDPRelationFile;

#define NDP_TRUE 1
#define NDP_FALSE 0

/* TYPE1 - for static table e.g) NATION */
#define VI_TYPE1 0 
/* TYPE2 - for insert-only table e.g) ORDERLINE */
#define VI_TYPE2 1 
/* TYPE3 - for table with update workload */
#define VI_TYPE3 2

extern Oid target_version_index;

void NDPSupportModuleDsaInit(void);

void NDPSupportModuleDetachDsa(void);

Size NDPSupportModuleShmemSize(void);

void NDPSupportModuleInit(void);

void TransferScanInfoForNDP(PlanState * node, Oid oid);

void TransferJoinInfoForNDP(PlanState * node, List *tl);

int TransferJoinInfoForNDPwithVersionIndex(PlanState * node, List *tl, 
		NDPRelationFile* files);

uint32_t TransferOperationInfo(PlanState * node, uint32 total_len, 
                               uint32 num_rel, uint16 *column_info, 
                               int *fds_arr);

void FlushVersionIndexBySegment(PlanState *node, NDPRelationFile files, int nrel);

#ifdef SMARTSSD_TIME_PROFILE
void PrintProfileLog(const char * log_comment);
#endif

#endif //ifndef NDP_SUPPORT_H

#endif //#ifdef SMARTSSD
