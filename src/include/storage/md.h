/*-------------------------------------------------------------------------
 *
 * md.h
 *	  magnetic disk storage manager public interface declarations.
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/md.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MD_H
#define MD_H

#include "storage/block.h"
#include "storage/relfilenode.h"
#include "storage/smgr.h"
#include "storage/sync.h"

/* md storage manager functionality */
extern void mdinit(void);
extern void mdclose(SMgrRelation reln, ForkNumber forknum);
extern void mdcreate(SMgrRelation reln, ForkNumber forknum, bool isRedo);
extern bool mdexists(SMgrRelation reln, ForkNumber forknum);
extern void mdunlink(RelFileNodeBackend rnode, ForkNumber forknum, bool isRedo);
extern void mdextend(SMgrRelation reln, ForkNumber forknum,
					 BlockNumber blocknum, char *buffer, bool skipFsync);
extern void mdprefetch(SMgrRelation reln, ForkNumber forknum,
					   BlockNumber blocknum);
extern void mdread(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
				   char *buffer);
extern void mdwrite(SMgrRelation reln, ForkNumber forknum,
					BlockNumber blocknum, char *buffer, bool skipFsync);
extern void mdwriteback(SMgrRelation reln, ForkNumber forknum,
						BlockNumber blocknum, BlockNumber nblocks);
extern BlockNumber mdnblocks(SMgrRelation reln, ForkNumber forknum);
extern void mdtruncate(SMgrRelation reln, ForkNumber forknum,
					   BlockNumber nblocks);
#ifdef SMARTSSD_KT
extern int mdgetsegnumandrelativepagenum(SMgrRelation reln, ForkNumber forknum,
                                         BlockNumber blkno, 
                                         BlockNumber* p_targetseg);
extern int mdgetmaxsegmentnum(SMgrRelation reln, ForkNumber forknum);
extern int mdwritevi(SMgrRelation reln, ForkNumber forknum, BlockNumber segnum, 
                     char *buffer, int size_vi, off_t seekpos);
extern int mdwritevisync(SMgrRelation reln, ForkNumber forknum, 
                         BlockNumber segnum);
extern int mdgetsegfd(SMgrRelation reln, ForkNumber forknum, 
                        BlockNumber blkno);
extern int mdgetvifd(SMgrRelation reln, ForkNumber forknum, BlockNumber segnum);
extern int mdgetvifdidx(SMgrRelation reln, ForkNumber forknum, 
                        BlockNumber segnum);
#endif // #ifdef SMARTSSD
#ifdef SMARTSSD
extern int mdsyncseg(SMgrRelation reln, ForkNumber forknum, int fd);
extern int mdgetsegfd(SMgrRelation reln, ForkNumber forknum, BlockNumber segno);
#endif
#ifdef WSUL_CTID_TEST
extern int mdopenCTIDs(SMgrRelation reln);
extern size_t mdwriteCTID(SMgrRelation reln, BlockNumber blocknum, 
		char *buffer, uint32 len, off_t pos);
extern int mdcloseCTIDs(SMgrRelation reln);
extern int mdgetfd(SMgrRelation reln, ForkNumber forknum, 
                        BlockNumber segno);
#endif //WSUL_CTID_TEST

extern void mdimmedsync(SMgrRelation reln, ForkNumber forknum);

extern void ForgetDatabaseSyncRequests(Oid dbid);
extern void DropRelationFiles(RelFileNode *delrels, int ndelrels, bool isRedo);

/* md sync callbacks */
extern int	mdsyncfiletag(const FileTag *ftag, char *path);
extern int	mdunlinkfiletag(const FileTag *ftag, char *path);
extern bool mdfiletagmatches(const FileTag *ftag, const FileTag *candidate);

#endif							/* MD_H */
