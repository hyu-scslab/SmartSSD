/*---------------------------------------------------------------------
 *
 *
 * ndp_support.c
 *    set routines to track information for smart ssd.
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *    src/backend/storage/ssd/smart_ssd.c
 *---------------------------------------------------------------------
 */ 
#ifdef SMARTSSD

#include "postgres.h"
#include "storage/ndp_support.h"
#include "catalog/pg_type.h"
#include "catalog/pg_operator.h"

#include <unistd.h>
#include <pthread.h>
#include <string.h>
#include <time.h>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "access/table.h"
#include "executor/executor.h"
#include "storage/ndp_dirty_bufid.h"
#include "storage/fd.h"
#include "storage/md.h"
#include "storage/ndp_snapshot_info.h"
#include "storage/ndp_table_info.h"
#include "storage/ndp_version_index_cache.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/guc.h" // to import name of the smartssd_kernel_file
#include "storage/ndp_functions.h"
#include "storage/smgr.h"

#include "storage/vi.h"

void NDPSupportModuleAttachDsa(void);
void *thread_func(void *p);

extern Oid			MyDatabaseId;
extern Oid			MyDatabaseTableSpace;
extern int 			logfile_fd;
extern int			MyProcPid;

int64_t num_flush;

typedef struct thread_param
{
	void *ndpo;
} thread_param;

void 
NDPSupportModuleAttachDsa(void)
{
	// NDPAttachDsaDirtyBufIDs();
	// VersionIndexAttachDsa();
}

void NDPSupportModuleDetachDsa(void)
{
	// NDPDetachDsaDirtyBufIDs();
	// VersionIndexDetachDsa();
}

/*
 * NDPSupportModuleInitDsa
 *
 * Initialize a dsa area for NDP module.
 * This function should only be called after the initialization of 
 * the dynamic shared memory facilities.
 */
void
NDPSupportModuleDsaInit(void)
{
#if 0
	// dsa_area *dsa_buf_id_set; 
	// dsa_area *dsa_version_index;
	dsa_handle handle_buf_id_set; 
	// dsa_handle handle_version_index;
	uint32 expected = 0;

	/*
	 * The first backend process creates the dsa area for dirty buffer id list,
	 * and another backend processes waits the creation and then attach to it.
	 */
	if (pg_atomic_read_u32(&ProcGlobal->dirty_buf_id_set_dsa_handle) == 0)
	{
		if (pg_atomic_compare_exchange_u32(&ProcGlobal->dirty_buf_id_set_dsa_handle,
				&expected, UINT32_MAX))
		{
			/* This process is selected to create dsa_area itself */
			
			/* Initialize dsa area for NDP module */
			// dsa_buf_id_set = dsa_create(LWTRANCHE_DIRTY_BUFIDSET_DSA);
	 		// dsa_version_index = dsa_create(LWTRANCHE_VERSION_INDEX_DSA);

			// handle_buf_id_set = dsa_get_handle(dsa_buf_id_set);
			// handle_version_index = dsa_get_handle(dsa_version_index);

			// dsa_pin(dsa_buf_id_set);
			// dsa_pin(dsa_version_index);

			// NDPInitFreeNodeStack(dsa_buf_id_set);

			// dsa_detach(dsa_buf_id_set);
			// dsa_detach(dsa_version_index);

			pg_memory_barrier();
			
			/*
			pg_atomic_write_u32_impl(&ProcGlobal->version_index_dsa_handle, 
			handle_version_index);
			*/ 

			pg_memory_barrier();

			pg_atomic_write_u32_impl(&ProcGlobal->dirty_buf_id_set_dsa_handle, 
					handle_buf_id_set);
		}
	} 
	while (pg_atomic_read_u32(&ProcGlobal->dirty_buf_id_set_dsa_handle) == 
				 UINT32_MAX)
	{
		/*
		 * Another process is creating an initial dsa area for ndp module,
		 * so just wait it to finish and then attach to it.
		 */
		usleep(1);
	}

	NDPSupportModuleAttachDsa();
#endif
}

void *thread_func(void *p)
{
#ifdef SMARTSSD_DEBUG
	fprintf(stderr, "[S3D_THREAD] START\n");
#endif	

	// Scan_Join_SSD(((thread_param *)p)->ndpo);
	// Scan_Join_SSD_ooo(((thread_param *)p)->ndpo);
#ifdef SMARTSSD_DEBUG
	fprintf(stderr, "[S3D_THREAD] END\n");
#endif	
	sleep(1);

	return NULL;
}

/*
 * NDPSupportModuleShmemSize
 *
 * compute the size of shared memory of version entries
 */
Size 
NDPSupportModuleShmemSize(void)
{
	Size size = 0;

	size = add_size(size, NDPBufIDSetShmemSize());
	// size = add_size(size, VersionIndexShmemSize());

	return size;
}

/*
 * NDPSupportModuleInit
 *
 * Initialize the interface of smart ssd component;
 */
void
NDPSupportModuleInit(void)
{
	NDPBufIDSetInit();
	// VersionIndexInit();
}

void 
TransferScanInfoForNDP(PlanState * node, Oid oid)
{
	// snap_con = GetSnapshotContainer(node->state->es_snapshot);
	/* This is to transfer the snapshot information required 
		 for accessing correct versions in the target table to SmartSSDs. */
	//SSDTransferSnapshotInfo(snap_con);
	
	/* To flush dirty buffers of relation and extract column's information, 
		 we need the information in Relation structure. 
		 For these, get Relation structure from relation id */
	// relation = RelationIdGetRelation(relation_id);

	/* This is to flush dirty pages belonging to the target table 
		 since we have to ensure that dirty pages containing versions must be 
		 in the storage for safe access in the SmartSSD. 
		 IMPORTANT: This function must be invoked after we collect entry pointers 
		 and before we transfer the information to the SmartSSD. */
	// FlushRelationBuffersForNDP(node->state->es_relations[0]); 

	/* The following part obtains the entry pointers for records in the target table. */
	// item_con = GetItemPointerContainer(node->state->es_relations[0]->rd_id);
 
 
	/* This is to transfer the entry pointers for the target table to SmartSSDs. */
	//SSDTransferItemPointerEntry(item_con);

	// TODO: required TransferTableInfo to transfer the return of GetOpInfo
	// GetOpInfo(node->ps_ProjInfo)
}

/*
void 
SSDTransferSnapshotInfo(SnapshotContainer* snapshot_con) {
}
*/

/*
NDPTableInfo* 
SSDTransferItemPointerEntry(ItemPointerContainer* item_con) {
	NDPTableInfo* tableInfo = (NDPTableInfo*) malloc(sizeof(NDPTableInfo));
	return tableInfo;
}
*/

uint32_t
TransferOperationInfo(PlanState * node, uint32 total_len, uint32 num_rel, 
		uint16 *column_info, int *fds_arr)
{
	void *NDPCtl_START; 
	char *ptr;
	int cur_seg_num;
	int idx;
	int ret;

	Assert(smartssd_kernel_file != NULL);

	// shared memory for NDP COMMAND
	// TODO: we need to allocate this region visible to SmartSSD
	NDPCtl_START = malloc(total_len);
	memset(NDPCtl_START, 0, total_len);

	// Command
	ptr = (char*) NDPCtl_START;
	ptr += 4;

	// Length of message
	*((uint32*) ptr) = total_len;
	ptr += 4;

	// Number of transferred relation info
	*((uint32*) ptr) = num_rel;
	ptr += 4;

	for (idx = 0 ; idx < num_rel ; idx++)
	{
		*((uint16*) ptr) = *column_info;
		ptr += 2;
		column_info++;
		*((uint16*) ptr) = *column_info;
		ptr += 2;
		column_info++;
		
		cur_seg_num = *((int*) ptr) = *fds_arr;
		ptr += 4;
		fds_arr++;

		memcpy(ptr, fds_arr, sizeof(int) * cur_seg_num * 2);
		ptr += sizeof(int) * cur_seg_num * 2; 
		fds_arr += cur_seg_num * 2;
	}

	// Send message
 
	// node->state->es_fpga_mgr = FPGAManager_GetInstance(smartssd_kernel_file, true);
	// node->state->es_ndpo = NDPObject_GetInstance(node->state->es_fpga_mgr, NDPCtl, ""); 

	node->state->es_thread_param = malloc(sizeof(thread_param));
	((thread_param *)node->state->es_thread_param)->ndpo = node->state->es_ndpo;
	if ((ret = pthread_create(&node->state->es_s3d_thread, NULL, thread_func, 
		(void *)node->state->es_thread_param)) != 0)
		elog(ERROR, "pthread_create failed");
	
	//sleep(10);

	/*for (i = 0 ; i < (int)(tl->length >> 1); i++)
	{
		pos = 0;
		packet = (char *)malloc(4 * 1024 * 1024);
		while (true) 
		{			
			total_size = Get_Packets(node->state->es_ndpo, i, packet, (int)(pos / (4 * 1024 * 1024)));
			fprintf(stderr, "[SMARTSSD] packet #%d (num_tuples = %d, length = %d), %s %d %s\n", 
				(int)(pos / (4 * 1024 * 1024)), *((uint32 *)packet), 
				*((uint16_t *)(packet + 4)), __FILE__, __LINE__, __func__);
			pos += 4 * 1024 * 1024;
			if (pos >= total_size) 
				break;
		}		
		free(packet);
	}*/	

	/* TODO: After transfer those information to SmartSSD, 
		 SHOULD release the memory region of NDPCtl */
	free(NDPCtl_START);

 
	return total_len;
}

static
void
CalculateNDPCommandMessageLength(PlanState* node, List *tl, 
		uint32* _message_len, uint32* _files_len)
{
	uint32 total_len, files_len;
	Relation rel;
	ListCell *lc;
	s3d_hj_clause_filter* cf;
	total_len = sizeof(NDPCommandMessageData); 
	files_len = 0;

	foreach (lc, tl)
	{
		cf = (s3d_hj_clause_filter *) lfirst(lc);
#ifdef SMARTSSD_DEBUG
		fprintf(stderr, "rel: %d.%d\n", cf->inner_relid, linitial_oid(cf->inner_attnum_list));
		fprintf(stderr, "rel: %d.%d\n", cf->outer_relid, linitial_oid(cf->outer_attnum_list));
#endif //SMARTSSD_DEBUG

		// outer
		rel = RelationIdGetRelation(cf->outer_relid);
		total_len += sizeof(NDPRelationInfoData);
		total_len += (sizeof(uint16) * rel->rd_att->natts);
		// segment files 
		files_len += sizeof(NDPRelationFileData);
		files_len += (sizeof(NDPRelationFileSegment) 
				* rel->rd_smgr->md_num_open_segs[MAIN_FORKNUM]);
		RelationClose(rel); 

		// inner
		rel = RelationIdGetRelation(cf->inner_relid);
		total_len += sizeof(NDPRelationInfoData);
		total_len += (sizeof(uint16) * rel->rd_att->natts);
		// segment files 
		files_len += sizeof(NDPRelationFileData);
		files_len += (sizeof(NDPRelationFileSegment) 
				* rel->rd_smgr->md_num_open_segs[MAIN_FORKNUM]);
		RelationClose(rel);

		// filters
		if (cf->filter_list) {
			total_len += (cf->filter_list->length * sizeof(NDPFilter));
		}
		
	}
	*_message_len = total_len;
	*_files_len = files_len;
}

static
int
BuildNDPRelationInfo(Oid rel_id, List* att_list, List* filter_list, 
		int nrel, NDPCommandMessage message, NDPRelationFile* pfile, uint32* offset) 
{
	Relation relation;
	int i, natts, nseg;
	NDPRelationInfo ndp_relation;
	NDPRelationFile file;
	ListCell *lc, *la;
	s3d_filter *filter;
	char* c_value;
	int val_len;
	int scanattnum;

	relation = RelationIdGetRelation(rel_id);
	ndp_relation = (NDPRelationInfo) 
		(((char*) &message->relations[nrel++]) + *offset);
	nseg = rel_id == 16420 ? 1 : relation->rd_smgr->md_num_open_segs[MAIN_FORKNUM];
	ndp_relation->rel_id = rel_id;
	ndp_relation->num_segs = nseg;
	natts = relation->rd_att->natts;
	ndp_relation->column_info.num_att = natts;
	for (i = 0; i < natts; i++) {
		ndp_relation->column_info.attlen[i] = relation->rd_att->attrs[i].attlen;
		*offset += sizeof(uint16);
	}
	ndp_relation = (NDPRelationInfo) 
		((char*) ndp_relation + (sizeof(uint16) * natts));

	ndp_relation->column_info.hash_key_len = 0;
	ndp_relation->filter_info.num_filter = 0;

	// to regard three columns (i.e., o_c_id, o_d_id, o_w_id) as a single column
	foreach(la, att_list) {
		scanattnum = lfirst_oid(la);
		ndp_relation->column_info.hash_key_len +=
			relation->rd_att->attrs[scanattnum - 1].attlen;
		ndp_relation->column_info.hash_key_index = scanattnum;
	}

	if (filter_list) {
		i = 0;
		foreach (lc, filter_list) {
			filter = (s3d_filter *) lfirst(lc);
			if (filter->relid == rel_id) {
				switch (filter->opno) {
					//case 675: // ?? i_price between 1 and 400000 (float8B)
					//case 673: // ?? i_price between 1 and 400000 (float8B)
					case 2065: // ?? ">=" operator
						ndp_relation->filter_info.filters[i].op_type = NDP_FILTER_GTE;
						break;
					case 521: // ??? ">" operator
						ndp_relation->filter_info.filters[i].op_type = NDP_FILTER_GT;
						break;
					case 523: // ol_quantity <= 10
					case 540: // c_phone_p <= 7 (4B)
						ndp_relation->filter_info.filters[i].op_type = NDP_FILTER_LTE;
						break;
					case 525: // ol_quantity >= 1 
						ndp_relation->filter_info.filters[i].op_type = NDP_FILTER_GTE;
						break;
					case 534: // c_phone_p < 8 (4B)
					case Int4LessOperator:
					case Int8LessOperator:
						ndp_relation->filter_info.filters[i].op_type = NDP_FILTER_LT;
						break;
					case 1054: // n_name = 'Germany'
					case Int4EqualOperator:
					case TextEqualOperator:
					case OID_TEXT_LIKE_OP:// 1209
					case OID_BPCHAR_LIKE_OP:// 1211
						ndp_relation->filter_info.filters[i].op_type = NDP_FILTER_EQ;
						break;
					default: 
						ndp_relation->filter_info.filters[i].op_type = NDP_FILTER_EQ;
						fprintf(stderr, "rel: %d.%d op type: %d!!!???\n",
								rel_id, filter->attnum, filter->opno);
						break;
				}
#if SMARTSSD_DEBUG
				fprintf(stderr, "[SMARTSSD] Accepting opno: %d as %d\n", filter->opno,
						ndp_relation->filter_info.filters[i].op_type);
#endif //SMARTSSD_DEBUG

				ndp_relation->filter_info.filters[i].prefix_len = 0;
				ndp_relation->filter_info.filters[i].operand = filter->constval->constvalue;
				switch (filter->constval->consttype) {
					case TEXTOID: // 25
					case BPCHAROID:// 1042
					case VARCHAROID:
						c_value = TextDatumGetCString(filter->constval->constvalue);
						val_len = strlen(c_value);
						if (c_value[val_len - 1] == '%') {
							// Converting prefix match to exact match
							c_value[val_len - 1] = 0;
							val_len--;
						}
						ndp_relation->filter_info.filters[i].attr_type = NDP_FILTER_STRING;
						ndp_relation->filter_info.filters[i].prefix_len = val_len;
						ndp_relation->filter_info.filters[i].operand = 0UL;
						memcpy(&ndp_relation->filter_info.filters[i].operand, c_value, val_len);
						pfree(c_value);
						break;
					case INT8OID:
					case INT2OID:
					case CIDOID: //29
					case INT4OID:
						val_len = relation->rd_att->attrs[filter->attnum - 1].attlen;
						ndp_relation->filter_info.filters[i].prefix_len = val_len;
						switch (val_len) {
						 case 8:
						  ndp_relation->filter_info.filters[i].attr_type = NDP_FILTER_8B;
						  break;
						 case 4:
						  ndp_relation->filter_info.filters[i].attr_type = NDP_FILTER_4B;
						  break;
						 case 2:
						  ndp_relation->filter_info.filters[i].attr_type = NDP_FILTER_2B;
						  break;

						}
						break;
/*
					case INT8OID:
						ndp_relation->filter_info.filters[i].prefix_len = 8;
						ndp_relation->filter_info.filters[i].attr_type = NDP_FILTER_8B;
						break;
					case INT2OID:
						ndp_relation->filter_info.filters[i].prefix_len = 2;
						ndp_relation->filter_info.filters[i].attr_type = NDP_FILTER_2B;
						break;
					case CIDOID: //29
					case INT4OID:
						ndp_relation->filter_info.filters[i].prefix_len = 4;
						ndp_relation->filter_info.filters[i].attr_type = NDP_FILTER_4B;
					 	break;
*/
					case TIMESTAMPOID: // o_entry_d >= '2015-01-02 00:00:00.000000'
						ndp_relation->filter_info.filters[i].prefix_len = 8;
						ndp_relation->filter_info.filters[i].attr_type = NDP_FILTER_8B;
						break;
				}

				ndp_relation->filter_info.filters[i].target_idx = filter->attnum;
				ndp_relation->filter_info.num_filter = ++i;
				*offset += sizeof(NDPFilter);
			}
		}
	}
	/* files */
	file = *pfile;
	file->id = rel_id;
	file->num_segs = nseg;
	for (i = 0; i < nseg; i++) {
		file->seg[i].data = smgrgetsegfd(relation->rd_smgr, MAIN_FORKNUM, i); 
#ifdef WSUL_CTID_TEST
		file->seg[i].ctid = smgrgetsegfd(relation->rd_smgr, VERSION_INDEX_TEST, i);
#endif
	}
	*pfile = (NDPRelationFile) (((char*) ++file) + (nseg * sizeof(NDPRelationFileSegment)));
	RelationClose(relation);
	return nrel;
}

int
TransferJoinInfoForNDPwithVersionIndex(PlanState* node, List *tl, NDPRelationFile* ndp_files)
{
	int nrel;
	uint32 total_len, files_len; // used for total length of 
	uint32 offset;
	NDPCommandMessage message;
	NDPRelationFile file;
	ListCell* lc;
	s3d_hj_clause_filter *cf;

	offset = 0;
	CalculateNDPCommandMessageLength(node, tl, &total_len, &files_len);
	message = (NDPCommandMessage) palloc0(total_len);
	file = (NDPRelationFile) palloc0(files_len);
	*ndp_files = file;

	nrel = 0;
	message->command_type = NDP_COMMAND_JOIN;
	message->len = total_len;

	foreach (lc, tl)
	{
		cf = (s3d_hj_clause_filter *) lfirst(lc);
		// inner
		nrel = BuildNDPRelationInfo(cf->inner_relid, cf->inner_attnum_list,
			cf->filter_list, nrel, message, &file, &offset);
		// outer
		nrel = BuildNDPRelationInfo(cf->outer_relid, cf->outer_attnum_list,
			cf->filter_list, nrel, message, &file, &offset);
	}

	message->num_relations = nrel;

	Assert(smartssd_kernel_file != NULL);

	// call NDP kernel calls
	node->state->es_fpga_mgr = FPGAManager_GetInstance(smartssd_kernel_file, true);
	node->state->es_ndpo = NDPObject_GetInstance(node->state->es_fpga_mgr, (void *)message, "");

	// transfer files
	if (message) {
		pfree(message);
	}

	return nrel;
}

void
FlushVersionIndexBySegment(PlanState *node, NDPRelationFile files, int nrel)
{
	int i, j, table_index;
	NDPRelationFile file;
#ifdef VERSION_INDEX
	bool reuse;
#ifdef VERSION_INDEX_PERF
	uint64 vi_total;
	uint64 vi_visibility_check;
#endif	/* VERSION_INDEX_PERF */
#endif	/* VERSION_INDEX */
#ifdef SMARTSSD_DBUF
	FlushDirtyBufIDList dbuf_list;
	SMgrRelation reln;
	RelFileNode rnode;
	int flushed_rel_id_array[32] = {0, };
	int flushed_idx;
#endif
#ifdef SMARTSSD_TIME_PROFILE
	struct timespec start_prescreening, end_prescreening, start_dbuf_flush, 
					end_dbuf_flush, start_get_dbuflist, end_get_dbuflist,
					start_dbuf_total, end_dbuf_total, start_transfer_time,
					end_transfer_time, start_vi_sync, end_vi_sync, 
					start_vi_total, end_vi_total;
	double prescreening_time = 0, dbuf_flush_time = 0, dbuf_getlist_time = 0, 
		   dbuf_total_time = 0, vi_flush_time = 0, vi_total_time = 0, 
		   total_transfer_time = 0;
	char log_comment[1024] = {0, };

	
	clock_gettime(CLOCK_MONOTONIC, &start_transfer_time);
#endif

	for (i = 0, file = files; i < nrel; i++) {
#ifdef SMARTSSD_DBUF
		for (flushed_idx = 0 ; flushed_idx < 32 ; flushed_idx++)
		{
			if (flushed_rel_id_array[flushed_idx] == 0)
				break;

			if (flushed_rel_id_array[flushed_idx] == file->id)
				break;
		}
		rnode.relNode = file->id;
		rnode.dbNode = MyDatabaseId;
		rnode.spcNode = MyDatabaseTableSpace;
		reln = smgropen(rnode, InvalidBackendId);
#endif
		table_index = Get_TableIndex(node->state->es_ndpo, file->id);
		if (table_index == -1)
		{
			fprintf(stderr, "table_index error\n");
		}

#ifdef VERSION_INDEX
		/* Are we revisiting the relation in the same query? */
		reuse = (flushed_rel_id_array[flushed_idx] != 0);
#endif	/* VERSION_INDEX */

		for (j = 0; j < file->num_segs; j++) {
#ifdef VERSION_INDEX

#ifdef SMARTSSD_TIME_PROFILE 
			clock_gettime(CLOCK_MONOTONIC, &start_vi_total);
			clock_gettime(CLOCK_MONOTONIC, &start_prescreening);
#endif

#ifdef VERSION_INDEX_PERF
			VI_INIT_PERF_CONTEXT(vi_perf_ctx);
			clock_gettime(CLOCK_MONOTONIC, &vi_perf_ctx.start);
#endif	/* VERSION_INDEX_PERF */
			/*
			 * Creates a version index file corresponding to the relation and
			 * segment ID. The prescreened version indexes are saved into a
			 * version index file with a suffix '.i' (e.g., 16402.0.i). Then,
			 * the file descriptor to the version index file is opened and
			 * returned. The caller is responsible for closing the file
			 * descriptor.
			 */
			file->seg[j].ctid =
				ViPrescreen(file->id, j, node->state->es_snapshot, reuse);
#ifdef VERSION_INDEX_PERF
			clock_gettime(CLOCK_MONOTONIC, &vi_perf_ctx.stop);
			vi_total = TIMESPEC_TO_MS(vi_perf_ctx.stop) -
				TIMESPEC_TO_MS(vi_perf_ctx.start);
			vi_visibility_check = vi_perf_ctx.visibility_check;
			fprintf(stderr, "[ViPerf] rel_id: %u, seg_id: %u, total: %lu, visibility: %lu, memcpy: %lu, pwrite: %lu, percentage: %lf\n",
					file->id, j,
					vi_total, vi_visibility_check,
					vi_perf_ctx.memcpy_time,
					vi_perf_ctx.pwrite_time,
					(double)vi_visibility_check / (double)vi_total);
#endif	/* VERSION_INDEX_PERF */

#ifdef SMARTSSD_TIME_PROFILE
			clock_gettime(CLOCK_MONOTONIC, &end_prescreening);
			prescreening_time += end_prescreening.tv_sec - 
							 	start_prescreening.tv_sec +
								(end_prescreening.tv_nsec - 
								 start_prescreening.tv_nsec) / 1000000000.0;
#endif
#ifdef SMARTSSD_TIME_PROFILE 
			clock_gettime(CLOCK_MONOTONIC, &start_vi_sync);
#endif
			/* This version file is not valid, we also set seg[j].data as -2
				 to skip this segment for NDP kernels. */
			if (file->seg[j].ctid == -2) {
				file->seg[j].data = -2;
			} else {
				fsync(file->seg[j].ctid);
			}
#endif	/* VERSION_INDEX*/

#ifdef SMARTSSD_TIME_PROFILE 
			clock_gettime(CLOCK_MONOTONIC, &end_vi_sync);
			clock_gettime(CLOCK_MONOTONIC, &end_vi_total);

			vi_flush_time += end_vi_sync.tv_sec - start_vi_sync.tv_sec +
							(end_vi_sync.tv_nsec - 
							 start_vi_sync.tv_nsec) / 1000000000.0;

			vi_total_time += end_vi_total.tv_sec - start_vi_total.tv_sec +
							(end_vi_total.tv_nsec - start_vi_total.tv_nsec)
							/ 1000000000.0;
#endif

#ifdef SMARTSSD_DBUF
#ifdef SMARTSSD_TIME_PROFILE
			clock_gettime(CLOCK_MONOTONIC, &start_dbuf_total);
#endif
			// 1) flush dirty buffers given segment
			if (file->seg[j].ctid != -2 && 
				!flushed_rel_id_array[flushed_idx]) {
#ifdef SMARTSSD_TIME_PROFILE
				clock_gettime(CLOCK_MONOTONIC, &start_get_dbuflist);
#endif
				dbuf_list = ndpsmgrdetachdirtybufid(file->id, j);

#ifdef SMARTSSD_TIME_PROFILE
				clock_gettime(CLOCK_MONOTONIC, &end_get_dbuflist);
				dbuf_getlist_time += (end_get_dbuflist.tv_sec - 
									 start_get_dbuflist.tv_sec) +
									 (end_get_dbuflist.tv_nsec -
									 start_get_dbuflist.tv_nsec) / 1000000000.0;
#endif

#ifdef SMARTSSD_TIME_PROFILE
				clock_gettime(CLOCK_MONOTONIC, &start_dbuf_flush);
#endif
		
				if (dbuf_list) {
					FlushSegmentBuffersForNDP(dbuf_list, reln);
				}

				// 2) sync a segment file
				fsync(file->seg[j].data);
#ifdef SMARTSSD_TIME_PROFILE
				clock_gettime(CLOCK_MONOTONIC, &end_dbuf_flush);
				dbuf_flush_time += (end_dbuf_flush.tv_sec - 
								   start_dbuf_flush.tv_sec) + 
								   (end_dbuf_flush.tv_nsec - 
								   start_dbuf_flush.tv_nsec) / 
								   1000000000.0;
#endif
			}
#endif
				
#ifdef SMARTSSD_TIME_PROFILE
			clock_gettime(CLOCK_MONOTONIC, &end_dbuf_total);
			dbuf_total_time += (end_dbuf_total.tv_sec - 
								start_dbuf_total.tv_sec) +
							   (end_dbuf_total.tv_nsec - 
								start_dbuf_total.tv_nsec) / 1000000000.0;
#endif

			// Then, what we will do?
			FlushVersionIndex(node->state->es_ndpo, table_index, 1 
					/*file->num_segs*/, &file->seg[j].data, &file->seg[j].ctid);
		}
#ifdef SMARTSSD_DBUF
		flushed_rel_id_array[flushed_idx] = file->id;
#endif
		file = (NDPRelationFile) (((char*) file) + 
				(file->num_segs * sizeof(NDPRelationFileSegment)));
		file++;
	}
#ifdef SMARTSSD_TIME_PROFILE
	clock_gettime(CLOCK_MONOTONIC, &end_transfer_time);
	total_transfer_time = (end_transfer_time.tv_sec - start_transfer_time.tv_sec) +
						  (end_transfer_time.tv_nsec - 
						   start_transfer_time.tv_nsec) / 1000000000.0;
	
	sprintf(log_comment, "%ld %.3lf %.3lf %.3lf %.3lf %.3lf %.3lf %.3lf ", 
			num_flush++, (double)total_transfer_time * 1000,
		   	(double)vi_total_time * 1000, (double)prescreening_time * 1000,
		    (double)vi_flush_time * 1000, (double)dbuf_total_time * 1000,
		    (double)dbuf_getlist_time * 1000, (double)dbuf_flush_time * 1000);

	PrintProfileLog(log_comment);
#endif
}

#ifdef SMARTSSD_TIME_PROFILE
void 
PrintProfileLog(const char * log_comment)
{
	if (!logfile_fd)
	{
		char target_dir[4096];
		char * ptr;
		time_t cur_time = time(NULL);
		struct tm tm= *localtime(&cur_time);
		const char * init_string = "# Times, Total transfer , Total version index, Prescreening, Vi transfer, total dirty buffer, dirty buffer id array, dirty buffer flush, total query\n";

		getcwd(target_dir, sizeof(target_dir));
		ptr = strstr(target_dir, "data");
		sprintf(ptr, "logs/");	
		*(ptr + strlen("logs/")) = 0;

		if (access(target_dir, 0))
		{
			mkdir(target_dir, 0755);
		}
		ptr += strlen("logs/");
		sprintf(ptr, "%d-%d-%d_%d:%d:%d_proc[%d]", tm.tm_year+1900,
				tm.tm_mon+1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec,
				MyProcPid);
		logfile_fd = open(target_dir, O_WRONLY|O_CREAT|O_APPEND, 0644);
		write(logfile_fd, init_string, strlen(init_string));
	}
	write(logfile_fd, log_comment, strlen(log_comment));
}

#endif

#endif //ifdef SMARTSSD
