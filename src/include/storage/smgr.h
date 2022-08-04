/*-------------------------------------------------------------------------
 *
 * smgr.h
 *	  storage manager switch public interface declarations.
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/smgr.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SMGR_H
#define SMGR_H

#include "lib/ilist.h"
#include "storage/block.h"
#include "storage/relfilenode.h"

#ifdef SMARTSSD_DBUF
typedef int Buffer;
typedef struct FlushDirtyBufIDListData* FlushDirtyBufIDList;
#endif

/*
 * smgr.c maintains a table of SMgrRelation objects, which are essentially
 * cached file handles.  An SMgrRelation is created (if not already present)
 * by smgropen(), and destroyed by smgrclose().  Note that neither of these
 * operations imply I/O, they just create or destroy a hashtable entry.
 * (But smgrclose() may release associated resources, such as OS-level file
 * descriptors.)
 *
 * An SMgrRelation may have an "owner", which is just a pointer to it from
 * somewhere else; smgr.c will clear this pointer if the SMgrRelation is
 * closed.  We use this to avoid dangling pointers from relcache to smgr
 * without having to make the smgr explicitly aware of relcache.  There
 * can't be more than one "owner" pointer per SMgrRelation, but that's
 * all we need.
 *
 * SMgrRelations that do not have an "owner" are considered to be transient,
 * and are deleted at end of transaction.
 */
typedef struct SMgrRelationData
{
	/* rnode is the hashtable lookup key, so it must be first! */
	RelFileNodeBackend smgr_rnode;	/* relation physical identifier */

	/* pointer to owning pointer, or NULL if none */
	struct SMgrRelationData **smgr_owner;

	/*
	 * These next three fields are not actually used or manipulated by smgr,
	 * except that they are reset to InvalidBlockNumber upon a cache flush
	 * event (in particular, upon truncation of the relation).  Higher levels
	 * store cached state here so that it will be reset when truncation
	 * happens.  In all three cases, InvalidBlockNumber means "unknown".
	 */
	BlockNumber smgr_targblock; /* current insertion target block */
	BlockNumber smgr_fsm_nblocks;	/* last known size of fsm fork */
	BlockNumber smgr_vm_nblocks;	/* last known size of vm fork */

	/* additional public fields may someday exist here */

	/*
	 * Fields below here are intended to be private to smgr.c and its
	 * submodules.  Do not touch them from elsewhere.
	 */
	int			smgr_which;		/* storage manager selector */

	/*
	 * for md.c; per-fork arrays of the number of open segments
	 * (md_num_open_segs) and the segments themselves (md_seg_fds).
	 */
	int			md_num_open_segs[MAX_FORKNUM + 1];
	struct _MdfdVec *md_seg_fds[MAX_FORKNUM + 1];

#ifdef SMARTSSD_KT
	/*
	 * for version index; per-fork arrays of the number of open version index per
	 * segments.
	 */
	int	 md_num_open_vi[MAX_FORKNUM + 1];
	struct _MdfdVec *md_vi_fds[MAX_FORKNUM + 1];
#endif

	/* if unowned, list link in list of all unowned SMgrRelations */
	dlist_node	node;
} SMgrRelationData;

typedef SMgrRelationData *SMgrRelation;

#define SmgrIsTemp(smgr) \
	RelFileNodeBackendIsTemp((smgr)->smgr_rnode)

extern void smgrinit(void);
extern SMgrRelation smgropen(RelFileNode rnode, BackendId backend);
extern bool smgrexists(SMgrRelation reln, ForkNumber forknum);
extern void smgrsetowner(SMgrRelation *owner, SMgrRelation reln);
extern void smgrclearowner(SMgrRelation *owner, SMgrRelation reln);
extern void smgrclose(SMgrRelation reln);
extern void smgrcloseall(void);
extern void smgrclosenode(RelFileNodeBackend rnode);
extern void smgrcreate(SMgrRelation reln, ForkNumber forknum, bool isRedo);
extern void smgrdounlink(SMgrRelation reln, bool isRedo);
extern void smgrdounlinkall(SMgrRelation *rels, int nrels, bool isRedo);
extern void smgrdounlinkfork(SMgrRelation reln, ForkNumber forknum, bool isRedo);
extern void smgrextend(SMgrRelation reln, ForkNumber forknum,
						 BlockNumber blocknum, char *buffer, bool skipFsync);
extern void smgrprefetch(SMgrRelation reln, ForkNumber forknum,
						 BlockNumber blocknum);
extern void smgrread(SMgrRelation reln, ForkNumber forknum,
					 BlockNumber blocknum, char *buffer);
extern void smgrwrite(SMgrRelation reln, ForkNumber forknum,
						BlockNumber blocknum, char *buffer, bool skipFsync);
extern void smgrwriteback(SMgrRelation reln, ForkNumber forknum,
							BlockNumber blocknum, BlockNumber nblocks);
extern BlockNumber smgrnblocks(SMgrRelation reln, ForkNumber forknum);
extern void smgrtruncate(SMgrRelation reln, ForkNumber forknum,
						 BlockNumber nblocks);
extern void smgrimmedsync(SMgrRelation reln, ForkNumber forknum);
extern void AtEOXact_SMgr(void);
#ifdef SMARTSSD
extern BlockNumber smgrgetsegnumandrelativepagenum(SMgrRelation reln, 
													ForkNumber forknum, 
													BlockNumber blocknum, 
													BlockNumber* p_targetseg);
extern int smgrwritevi(SMgrRelation reln, ForkNumber forknum,
						BlockNumber segnum, char *buffer,
						int size_version_index, off_t seekpos);
extern int smgrwritevisync(SMgrRelation reln, ForkNumber forknum,
											     BlockNumber segnum);
extern int smgrgetvifd(SMgrRelation reln, ForkNumber forknum,
											 BlockNumber segnum);
extern int smgrgetvifdidx(SMgrRelation reln, ForkNumber forknum,
													BlockNumber segnum);
extern int smgrgetsegfd(SMgrRelation reln, ForkNumber forknum, 
												BlockNumber blocknum);
extern int smgrsyncseg(SMgrRelation reln, ForkNumber forknum, int fd);
#endif // #ifdef SMARTSSD

/**** Entry points of new buffer id list ****/
#ifdef SMARTSSD_DBUF
/* When page becomes dirty, set its ID to list */
extern void ndpsmgrsetdirtybufid(Buffer buf); 
/* When page flushed, pop its ID to list */
extern void ndpsmgrpopdirtybufid(Buffer buf); 
/* To flush dirty pages of relation, get the info of dirty page ids list */
extern FlushDirtyBufIDList ndpsmgrdetachdirtybufid(Oid relid, uint32 segno);
extern void ndpsmgrreleasedirtybufid(Oid relid, uint32 max_segno, bool remove);
/********************************************/
#endif

#ifdef WSUL_CTID_TEST
extern int smgrgetsegfd(SMgrRelation reln, ForkNumber forknum, 
		BlockNumber segno);
extern int smgropenCTIDs(SMgrRelation reln);
extern size_t smgrwriteCTID(SMgrRelation reln, BlockNumber blocknum, 
		char *buffer, uint32 len, off_t pos);
extern int smgrcloseCTIDs(SMgrRelation reln);
#endif //WSUL_CTID_TEST
#endif							/* SMGR_H */
