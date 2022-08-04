/*---------------------------------------------------------------------
 *
 *
 * ndp_table_info.c
 *    routines to manage table infos for ndp.
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *    src/backend/storage/ssd/ndp_table_info.c
 *---------------------------------------------------------------------
 */

#if defined(SMARTSSD)

#include "storage/ndp_table_info.h"
#include "storage/ndp_version_index_cache.h"

#include "utils/rel.h"
#include "utils/relcache.h"

#include "commands/tablecmds.h"
#include "nodes/execnodes.h"

/*
 * GetAttrSizeAndOffset
 *
 * get the offset and size of attributes
 */
uint16* GetAttrSizeAndOffset(Relation rel)
{
	int i;
	int natts = rel->rd_att->natts;			/* number of attributes in the tuple */
  uint16 off_col = 0;
  uint16 *off_len_arr = (uint16*)malloc(sizeof(uint16) * (natts * 2 + 1));
  *off_len_arr = (uint16)natts;
  off_len_arr++;

	for (i = 0 ; i < natts ; i++)
	{
    *off_len_arr = (uint16)off_col;
    off_len_arr++;
    *off_len_arr = (uint16)rel->rd_att->attrs[i].attlen;
    off_col += rel->rd_att->attrs[i].attlen;
    off_len_arr++;
	}

	return off_len_arr;
}

#endif
