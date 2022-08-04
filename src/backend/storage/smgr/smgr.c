/*-------------------------------------------------------------------------
 *
 * smgr.c
 *	  public interface routines to storage manager switch.
 *
 *	  All file system operations in POSTGRES dispatch through these
 *	  routines.
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/smgr/smgr.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "commands/tablespace.h"
#include "lib/ilist.h"
#include "storage/bufmgr.h"
#include "storage/ipc.h"
#include "storage/md.h"
#include "storage/smgr.h"
#include "utils/hsearch.h"
#include "utils/inval.h"

#ifdef SMARTSSD_DBUF
#include "storage/ndp_dirty_bufid.h"

#include "storage/buf_internals.h"
#include "storage/lmgr.h"
#include "storage/lwlock.h"
#include "access/relation.h"
#include "utils/rel.h"
#include "access/xact.h"
#include "catalog/catalog.h"

extern HTAB * SharedBufIDSetHash;
extern Oid MyDatabaseId;
#endif

/*
 * This struct of function pointers defines the API between smgr.c and
 * any individual storage manager module.  Note that smgr subfunctions are
 * generally expected to report problems via elog(ERROR).  An exception is
 * that smgr_unlink should use elog(WARNING), rather than erroring out,
 * because we normally unlink relations during post-commit/abort cleanup,
 * and so it's too late to raise an error.  Also, various conditions that
 * would normally be errors should be allowed during bootstrap and/or WAL
 * recovery --- see comments in md.c for details.
 */
typedef struct f_smgr
{
	void		(*smgr_init) (void);	/* may be NULL */
	void		(*smgr_shutdown) (void);	/* may be NULL */
	void		(*smgr_close) (SMgrRelation reln, ForkNumber forknum);
	void		(*smgr_create) (SMgrRelation reln, ForkNumber forknum,
								bool isRedo);
	bool		(*smgr_exists) (SMgrRelation reln, ForkNumber forknum);
	void		(*smgr_unlink) (RelFileNodeBackend rnode, ForkNumber forknum,
								bool isRedo);
	void		(*smgr_extend) (SMgrRelation reln, ForkNumber forknum,
								BlockNumber blocknum, char *buffer, bool skipFsync);
	void		(*smgr_prefetch) (SMgrRelation reln, ForkNumber forknum,
								  BlockNumber blocknum);
	void		(*smgr_read) (SMgrRelation reln, ForkNumber forknum,
							  BlockNumber blocknum, char *buffer);
	void		(*smgr_write) (SMgrRelation reln, ForkNumber forknum,
							   BlockNumber blocknum, char *buffer, bool skipFsync);
	void		(*smgr_writeback) (SMgrRelation reln, ForkNumber forknum,
								   BlockNumber blocknum, BlockNumber nblocks);
	BlockNumber (*smgr_nblocks) (SMgrRelation reln, ForkNumber forknum);
	void		(*smgr_truncate) (SMgrRelation reln, ForkNumber forknum,
								  BlockNumber nblocks);
	void		(*smgr_immedsync) (SMgrRelation reln, ForkNumber forknum);
#ifdef SMARTSSD_KT
	int		 (*smgr_getsegnumandrelativepagenum)(SMgrRelation reln, 
																							ForkNumber forknum,
																							BlockNumber blocknum, 
																							BlockNumber* p_targetseg);
	int		 (*smgr_getmaxsegmentnum)(SMgrRelation reln, ForkNumber forknum);
	int		 (*smgr_writevi)(SMgrRelation reln, ForkNumber forknum,
													BlockNumber segnum, char *buffer,
			 										int size_vi, off_t seekpos);
	int		 (*smgr_writevisync)(SMgrRelation reln, ForkNumber forknum,
													   BlockNumber segnum);
	int		 (*smgr_getvifd) (SMgrRelation reln, ForkNumber forknum,
														 BlockNumber segnum);
	int		 (*smgr_getvifdidx) (SMgrRelation reln, ForkNumber forknum,
														 BlockNumber segnum);
	int		 (*smgr_getsegfd) (SMgrRelation reln, ForkNumber forknum,
															BlockNumber blkno);
	int		 (*smgr_syncseg) (SMgrRelation reln, ForkNumber forknum, int fd);
#endif
#ifdef SMARTSSD
	int	(*smgr_getsegfd) (SMgrRelation reln, ForkNumber forknum, 
			BlockNumber segno);
#endif
#ifdef WSUL_CTID_TEST
	int (*smgr_openCTIDs) (SMgrRelation reln);
	size_t (*smgr_writeCTID) (SMgrRelation reln, BlockNumber blocknum, 
			char* buffer, uint32 len, off_t pos);
	int (*smgr_closeCTIDs) (SMgrRelation reln);
#endif //WSUL_CTID_TEST
} f_smgr;

static const f_smgr smgrsw[] = {
	/* magnetic disk */
	{
		.smgr_init = mdinit,
		.smgr_shutdown = NULL,
		.smgr_close = mdclose,
		.smgr_create = mdcreate,
		.smgr_exists = mdexists,
		.smgr_unlink = mdunlink,
		.smgr_extend = mdextend,
		.smgr_prefetch = mdprefetch,
		.smgr_read = mdread,
		.smgr_write = mdwrite,
		.smgr_writeback = mdwriteback,
		.smgr_nblocks = mdnblocks,
		.smgr_truncate = mdtruncate,
		.smgr_immedsync = mdimmedsync,
#ifdef SMARTSSD_KT
		.smgr_getsegnumandrelativepagenum = mdgetsegnumandrelativepagenum,
		.smgr_getmaxsegmentnum = mdgetmaxsegmentnum,
		.smgr_writevi = mdwritevi,
		.smgr_writevisync = mdwritevisync,
		.smgr_getvifd = mdgetvifd,
		.smgr_getvifdidx = mdgetvifdidx,
		.smgr_getsegfd = mdgetsegfd,
		.smgr_syncseg = mdsyncseg,
#endif
#ifdef SMARTSSD
		.smgr_getsegfd = mdgetsegfd,
#endif
#ifdef WSUL_CTID_TEST
		.smgr_openCTIDs = mdopenCTIDs,
		.smgr_writeCTID = mdwriteCTID,
		.smgr_closeCTIDs = mdcloseCTIDs,
#endif //WSUL_CTID_TEST
	}
};

static const int NSmgr = lengthof(smgrsw);

/*
 * Each backend has a hashtable that stores all extant SMgrRelation objects.
 * In addition, "unowned" SMgrRelation objects are chained together in a list.
 */
static HTAB *SMgrRelationHash = NULL;

static dlist_head unowned_relns;

/* local function prototypes */
static void smgrshutdown(int code, Datum arg);

/*
 *	smgrinit(), smgrshutdown() -- Initialize or shut down storage
 *								  managers.
 *
 * Note: smgrinit is called during backend startup (normal or standalone
 * case), *not* during postmaster start.  Therefore, any resources created
 * here or destroyed in smgrshutdown are backend-local.
 */
void
smgrinit(void)
{
	int			i;

	for (i = 0; i < NSmgr; i++)
	{
		if (smgrsw[i].smgr_init)
			smgrsw[i].smgr_init();
	}

	/* register the shutdown proc */
	on_proc_exit(smgrshutdown, 0);
}

/*
 * on_proc_exit hook for smgr cleanup during backend shutdown
 */
static void
smgrshutdown(int code, Datum arg)
{
	int			i;

	for (i = 0; i < NSmgr; i++)
	{
		if (smgrsw[i].smgr_shutdown)
			smgrsw[i].smgr_shutdown();
	}
}

/*
 *	smgropen() -- Return an SMgrRelation object, creating it if need be.
 *
 *		This does not attempt to actually open the underlying file.
 */
SMgrRelation
smgropen(RelFileNode rnode, BackendId backend)
{
	RelFileNodeBackend brnode;
	SMgrRelation reln;
	bool		found;

	if (SMgrRelationHash == NULL)
	{
		/* First time through: initialize the hash table */
		HASHCTL		ctl;

		MemSet(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(RelFileNodeBackend);
		ctl.entrysize = sizeof(SMgrRelationData);
		SMgrRelationHash = hash_create("smgr relation table", 400,
									   &ctl, HASH_ELEM | HASH_BLOBS);
		dlist_init(&unowned_relns);
	}

	/* Look up or create an entry */
	brnode.node = rnode;
	brnode.backend = backend;
	reln = (SMgrRelation) hash_search(SMgrRelationHash,
									  (void *) &brnode,
									  HASH_ENTER, &found);

	/* Initialize it if not present before */
	if (!found)
	{
		int			forknum;

		/* hash_search already filled in the lookup key */
		reln->smgr_owner = NULL;
		reln->smgr_targblock = InvalidBlockNumber;
		reln->smgr_fsm_nblocks = InvalidBlockNumber;
		reln->smgr_vm_nblocks = InvalidBlockNumber;
		reln->smgr_which = 0;	/* we only have md.c at present */

		/* mark it not open */
		for (forknum = 0; forknum <= MAX_FORKNUM; forknum++)
		{
			reln->md_num_open_segs[forknum] = 0;
#ifdef SMARTSSD_KT
			/* version index should be marked it's not opened yet. */
			reln->md_num_open_vi[forknum] = 0;
#endif //SMRATSSD
		}
		/* it has no owner yet */
		dlist_push_tail(&unowned_relns, &reln->node);
	}

	return reln;
}

/*
 * smgrsetowner() -- Establish a long-lived reference to an SMgrRelation object
 *
 * There can be only one owner at a time; this is sufficient since currently
 * the only such owners exist in the relcache.
 */
void
smgrsetowner(SMgrRelation *owner, SMgrRelation reln)
{
	/* We don't support "disowning" an SMgrRelation here, use smgrclearowner */
	Assert(owner != NULL);

	/*
	 * First, unhook any old owner.  (Normally there shouldn't be any, but it
	 * seems possible that this can happen during swap_relation_files()
	 * depending on the order of processing.  It's ok to close the old
	 * relcache entry early in that case.)
	 *
	 * If there isn't an old owner, then the reln should be in the unowned
	 * list, and we need to remove it.
	 */
	if (reln->smgr_owner)
		*(reln->smgr_owner) = NULL;
	else
		dlist_delete(&reln->node);

	/* Now establish the ownership relationship. */
	reln->smgr_owner = owner;
	*owner = reln;
}

/*
 * smgrclearowner() -- Remove long-lived reference to an SMgrRelation object
 *					   if one exists
 */
void
smgrclearowner(SMgrRelation *owner, SMgrRelation reln)
{
	/* Do nothing if the SMgrRelation object is not owned by the owner */
	if (reln->smgr_owner != owner)
		return;

	/* unset the owner's reference */
	*owner = NULL;

	/* unset our reference to the owner */
	reln->smgr_owner = NULL;

	/* add to list of unowned relations */
	dlist_push_tail(&unowned_relns, &reln->node);
}

/*
 *	smgrexists() -- Does the underlying file for a fork exist?
 */
bool
smgrexists(SMgrRelation reln, ForkNumber forknum)
{
	return smgrsw[reln->smgr_which].smgr_exists(reln, forknum);
}

/*
 *	smgrclose() -- Close and delete an SMgrRelation object.
 */
void
smgrclose(SMgrRelation reln)
{
	SMgrRelation *owner;
	ForkNumber	forknum;

	for (forknum = 0; forknum <= MAX_FORKNUM; forknum++)
		smgrsw[reln->smgr_which].smgr_close(reln, forknum);

	owner = reln->smgr_owner;

	if (!owner)
		dlist_delete(&reln->node);

	if (hash_search(SMgrRelationHash,
					(void *) &(reln->smgr_rnode),
					HASH_REMOVE, NULL) == NULL)
		elog(ERROR, "SMgrRelation hashtable corrupted");

	/*
	 * Unhook the owner pointer, if any.  We do this last since in the remote
	 * possibility of failure above, the SMgrRelation object will still exist.
	 */
	if (owner)
		*owner = NULL;
}

/*
 *	smgrcloseall() -- Close all existing SMgrRelation objects.
 */
void
smgrcloseall(void)
{
	HASH_SEQ_STATUS status;
	SMgrRelation reln;

	/* Nothing to do if hashtable not set up */
	if (SMgrRelationHash == NULL)
		return;

	hash_seq_init(&status, SMgrRelationHash);

	while ((reln = (SMgrRelation) hash_seq_search(&status)) != NULL)
		smgrclose(reln);
}

/*
 *	smgrclosenode() -- Close SMgrRelation object for given RelFileNode,
 *					   if one exists.
 *
 * This has the same effects as smgrclose(smgropen(rnode)), but it avoids
 * uselessly creating a hashtable entry only to drop it again when no
 * such entry exists already.
 */
void
smgrclosenode(RelFileNodeBackend rnode)
{
	SMgrRelation reln;

	/* Nothing to do if hashtable not set up */
	if (SMgrRelationHash == NULL)
		return;

	reln = (SMgrRelation) hash_search(SMgrRelationHash,
									  (void *) &rnode,
									  HASH_FIND, NULL);
	if (reln != NULL)
		smgrclose(reln);
}

/*
 *	smgrcreate() -- Create a new relation.
 *
 *		Given an already-created (but presumably unused) SMgrRelation,
 *		cause the underlying disk file or other storage for the fork
 *		to be created.
 *
 *		If isRedo is true, it is okay for the underlying file to exist
 *		already because we are in a WAL replay sequence.
 */
void
smgrcreate(SMgrRelation reln, ForkNumber forknum, bool isRedo)
{
	/*
	 * Exit quickly in WAL replay mode if we've already opened the file. If
	 * it's open, it surely must exist.
	 */
	if (isRedo && reln->md_num_open_segs[forknum] > 0)
		return;

	/*
	 * We may be using the target table space for the first time in this
	 * database, so create a per-database subdirectory if needed.
	 *
	 * XXX this is a fairly ugly violation of module layering, but this seems
	 * to be the best place to put the check.  Maybe TablespaceCreateDbspace
	 * should be here and not in commands/tablespace.c?  But that would imply
	 * importing a lot of stuff that smgr.c oughtn't know, either.
	 */
#ifdef WSUL_CTID_TEST
	if (forknum != VERSION_INDEX_TEST) {
		TablespaceCreateDbspace(reln->smgr_rnode.node.spcNode, 
				reln->smgr_rnode.node.dbNode, isRedo);
	}
#else //WSUL_CTID_TEST
	TablespaceCreateDbspace(reln->smgr_rnode.node.spcNode,
							reln->smgr_rnode.node.dbNode,
							isRedo);
#endif //WSUL_CTID_TEST

	smgrsw[reln->smgr_which].smgr_create(reln, forknum, isRedo);
}

/*
 *	smgrdounlink() -- Immediately unlink all forks of a relation.
 *
 *		All forks of the relation are removed from the store.  This should
 *		not be used during transactional operations, since it can't be undone.
 *
 *		If isRedo is true, it is okay for the underlying file(s) to be gone
 *		already.
 *
 *		This is equivalent to calling smgrdounlinkfork for each fork, but
 *		it's significantly quicker so should be preferred when possible.
 */
void
smgrdounlink(SMgrRelation reln, bool isRedo)
{
	RelFileNodeBackend rnode = reln->smgr_rnode;
	int			which = reln->smgr_which;
	ForkNumber	forknum;

	/* Close the forks at smgr level */
	for (forknum = 0; forknum <= MAX_FORKNUM; forknum++)
		smgrsw[which].smgr_close(reln, forknum);

	/*
	 * Get rid of any remaining buffers for the relation.  bufmgr will just
	 * drop them without bothering to write the contents.
	 */
	DropRelFileNodesAllBuffers(&rnode, 1);

	/*
	 * It'd be nice to tell the stats collector to forget it immediately, too.
	 * But we can't because we don't know the OID (and in cases involving
	 * relfilenode swaps, it's not always clear which table OID to forget,
	 * anyway).
	 */

	/*
	 * Send a shared-inval message to force other backends to close any
	 * dangling smgr references they may have for this rel.  We should do this
	 * before starting the actual unlinking, in case we fail partway through
	 * that step.  Note that the sinval message will eventually come back to
	 * this backend, too, and thereby provide a backstop that we closed our
	 * own smgr rel.
	 */
	CacheInvalidateSmgr(rnode);

	/*
	 * Delete the physical file(s).
	 *
	 * Note: smgr_unlink must treat deletion failure as a WARNING, not an
	 * ERROR, because we've already decided to commit or abort the current
	 * xact.
	 */
	smgrsw[which].smgr_unlink(rnode, InvalidForkNumber, isRedo);
}

/*
 *	smgrdounlinkall() -- Immediately unlink all forks of all given relations
 *
 *		All forks of all given relations are removed from the store.  This
 *		should not be used during transactional operations, since it can't be
 *		undone.
 *
 *		If isRedo is true, it is okay for the underlying file(s) to be gone
 *		already.
 *
 *		This is equivalent to calling smgrdounlink for each relation, but it's
 *		significantly quicker so should be preferred when possible.
 */
void
smgrdounlinkall(SMgrRelation *rels, int nrels, bool isRedo)
{
	int			i = 0;
	RelFileNodeBackend *rnodes;
	ForkNumber	forknum;

	if (nrels == 0)
		return;

	/*
	 * create an array which contains all relations to be dropped, and close
	 * each relation's forks at the smgr level while at it
	 */
	rnodes = palloc(sizeof(RelFileNodeBackend) * nrels);
	for (i = 0; i < nrels; i++)
	{
		RelFileNodeBackend rnode = rels[i]->smgr_rnode;
		int			which = rels[i]->smgr_which;

		rnodes[i] = rnode;

		/* Close the forks at smgr level */
		for (forknum = 0; forknum <= MAX_FORKNUM; forknum++)
			smgrsw[which].smgr_close(rels[i], forknum);
	}

	/*
	 * Get rid of any remaining buffers for the relations.  bufmgr will just
	 * drop them without bothering to write the contents.
	 */
	DropRelFileNodesAllBuffers(rnodes, nrels);

	/*
	 * It'd be nice to tell the stats collector to forget them immediately,
	 * too. But we can't because we don't know the OIDs.
	 */

	/*
	 * Send a shared-inval message to force other backends to close any
	 * dangling smgr references they may have for these rels.  We should do
	 * this before starting the actual unlinking, in case we fail partway
	 * through that step.  Note that the sinval messages will eventually come
	 * back to this backend, too, and thereby provide a backstop that we
	 * closed our own smgr rel.
	 */
	for (i = 0; i < nrels; i++)
		CacheInvalidateSmgr(rnodes[i]);

	/*
	 * Delete the physical file(s).
	 *
	 * Note: smgr_unlink must treat deletion failure as a WARNING, not an
	 * ERROR, because we've already decided to commit or abort the current
	 * xact.
	 */

	for (i = 0; i < nrels; i++)
	{
		int			which = rels[i]->smgr_which;

		for (forknum = 0; forknum <= MAX_FORKNUM; forknum++)
			smgrsw[which].smgr_unlink(rnodes[i], forknum, isRedo);
	}

	pfree(rnodes);
}

/*
 *	smgrdounlinkfork() -- Immediately unlink one fork of a relation.
 *
 *		The specified fork of the relation is removed from the store.  This
 *		should not be used during transactional operations, since it can't be
 *		undone.
 *
 *		If isRedo is true, it is okay for the underlying file to be gone
 *		already.
 */
void
smgrdounlinkfork(SMgrRelation reln, ForkNumber forknum, bool isRedo)
{
	RelFileNodeBackend rnode = reln->smgr_rnode;
	int			which = reln->smgr_which;

	/* Close the fork at smgr level */
	smgrsw[which].smgr_close(reln, forknum);

	/*
	 * Get rid of any remaining buffers for the fork.  bufmgr will just drop
	 * them without bothering to write the contents.
	 */
	DropRelFileNodeBuffers(rnode, forknum, 0);

	/*
	 * It'd be nice to tell the stats collector to forget it immediately, too.
	 * But we can't because we don't know the OID (and in cases involving
	 * relfilenode swaps, it's not always clear which table OID to forget,
	 * anyway).
	 */

	/*
	 * Send a shared-inval message to force other backends to close any
	 * dangling smgr references they may have for this rel.  We should do this
	 * before starting the actual unlinking, in case we fail partway through
	 * that step.  Note that the sinval message will eventually come back to
	 * this backend, too, and thereby provide a backstop that we closed our
	 * own smgr rel.
	 */
	CacheInvalidateSmgr(rnode);

	/*
	 * Delete the physical file(s).
	 *
	 * Note: smgr_unlink must treat deletion failure as a WARNING, not an
	 * ERROR, because we've already decided to commit or abort the current
	 * xact.
	 */
	smgrsw[which].smgr_unlink(rnode, forknum, isRedo);
}

/*
 *	smgrextend() -- Add a new block to a file.
 *
 *		The semantics are nearly the same as smgrwrite(): write at the
 *		specified position.  However, this is to be used for the case of
 *		extending a relation (i.e., blocknum is at or beyond the current
 *		EOF).  Note that we assume writing a block beyond current EOF
 *		causes intervening file space to become filled with zeroes.
 */
void
smgrextend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		   char *buffer, bool skipFsync)
{
	smgrsw[reln->smgr_which].smgr_extend(reln, forknum, blocknum,
										 buffer, skipFsync);
}

/*
 *	smgrprefetch() -- Initiate asynchronous read of the specified block of a relation.
 */
void
smgrprefetch(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum)
{
	smgrsw[reln->smgr_which].smgr_prefetch(reln, forknum, blocknum);
}

/*
 *	smgrread() -- read a particular block from a relation into the supplied
 *				  buffer.
 *
 *		This routine is called from the buffer manager in order to
 *		instantiate pages in the shared buffer cache.  All storage managers
 *		return pages in the format that POSTGRES expects.
 */
void
smgrread(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		 char *buffer)
{
	smgrsw[reln->smgr_which].smgr_read(reln, forknum, blocknum, buffer);
}

/*
 *	smgrwrite() -- Write the supplied buffer out.
 *
 *		This is to be used only for updating already-existing blocks of a
 *		relation (ie, those before the current EOF).  To extend a relation,
 *		use smgrextend().
 *
 *		This is not a synchronous write -- the block is not necessarily
 *		on disk at return, only dumped out to the kernel.  However,
 *		provisions will be made to fsync the write before the next checkpoint.
 *
 *		skipFsync indicates that the caller will make other provisions to
 *		fsync the relation, so we needn't bother.  Temporary relations also
 *		do not require fsync.
 */
void
smgrwrite(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		  char *buffer, bool skipFsync)
{
	smgrsw[reln->smgr_which].smgr_write(reln, forknum, blocknum,
										buffer, skipFsync);
}


/*
 *	smgrwriteback() -- Trigger kernel writeback for the supplied range of
 *					   blocks.
 */
void
smgrwriteback(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
			  BlockNumber nblocks)
{
	smgrsw[reln->smgr_which].smgr_writeback(reln, forknum, blocknum,
											nblocks);
}

/*
 *	smgrnblocks() -- Calculate the number of blocks in the
 *					 supplied relation.
 */
BlockNumber
smgrnblocks(SMgrRelation reln, ForkNumber forknum)
{
	return smgrsw[reln->smgr_which].smgr_nblocks(reln, forknum);
}

/*
 *	smgrtruncate() -- Truncate supplied relation to the specified number
 *					  of blocks
 *
 * The truncation is done immediately, so this can't be rolled back.
 */
void
smgrtruncate(SMgrRelation reln, ForkNumber forknum, BlockNumber nblocks)
{
	/*
	 * Get rid of any buffers for the about-to-be-deleted blocks. bufmgr will
	 * just drop them without bothering to write the contents.
	 */
	DropRelFileNodeBuffers(reln->smgr_rnode, forknum, nblocks);

	/*
	 * Send a shared-inval message to force other backends to close any smgr
	 * references they may have for this rel.  This is useful because they
	 * might have open file pointers to segments that got removed, and/or
	 * smgr_targblock variables pointing past the new rel end.  (The inval
	 * message will come back to our backend, too, causing a
	 * probably-unnecessary local smgr flush.  But we don't expect that this
	 * is a performance-critical path.)  As in the unlink code, we want to be
	 * sure the message is sent before we start changing things on-disk.
	 */
	CacheInvalidateSmgr(reln->smgr_rnode);

	/*
	 * Do the truncation.
	 */
	smgrsw[reln->smgr_which].smgr_truncate(reln, forknum, nblocks);
}

/*
 *	smgrimmedsync() -- Force the specified relation to stable storage.
 *
 *		Synchronously force all previous writes to the specified relation
 *		down to disk.
 *
 *		This is useful for building completely new relations (eg, new
 *		indexes).  Instead of incrementally WAL-logging the index build
 *		steps, we can just write completed index pages to disk with smgrwrite
 *		or smgrextend, and then fsync the completed index file before
 *		committing the transaction.  (This is sufficient for purposes of
 *		crash recovery, since it effectively duplicates forcing a checkpoint
 *		for the completed index.  But it is *not* sufficient if one wishes
 *		to use the WAL log for PITR or replication purposes: in that case
 *		we have to make WAL entries as well.)
 *
 *		The preceding writes should specify skipFsync = true to avoid
 *		duplicative fsyncs.
 *
 *		Note that you need to do FlushRelationBuffers() first if there is
 *		any possibility that there are dirty buffers for the relation;
 *		otherwise the sync is not very meaningful.
 */
void
smgrimmedsync(SMgrRelation reln, ForkNumber forknum)
{
	smgrsw[reln->smgr_which].smgr_immedsync(reln, forknum);
}

/*
 * AtEOXact_SMgr
 *
 * This routine is called during transaction commit or abort (it doesn't
 * particularly care which).  All transient SMgrRelation objects are closed.
 *
 * We do this as a compromise between wanting transient SMgrRelations to
 * live awhile (to amortize the costs of blind writes of multiple blocks)
 * and needing them to not live forever (since we're probably holding open
 * a kernel file descriptor for the underlying file, and we need to ensure
 * that gets closed reasonably soon if the file gets deleted).
 */
void
AtEOXact_SMgr(void)
{
	dlist_mutable_iter iter;

	/*
	 * Zap all unowned SMgrRelations.  We rely on smgrclose() to remove each
	 * one from the list.
	 */
	dlist_foreach_modify(iter, &unowned_relns)
	{
		SMgrRelation rel = dlist_container(SMgrRelationData, node,
										   iter.cur);

		Assert(rel->smgr_owner == NULL);

		smgrclose(rel);
	}
}

#ifdef SMARTSSD_KT
/*
 * smgrgetsegnumandrelativepagenum() -- get segment number of block and relative 
 * page number of block in segment
 * target segment number is saved in p_targetseg and relative page number of 
 * block is returned
 */
BlockNumber
smgrgetsegnumandrelativepagenum(SMgrRelation reln, ForkNumber forknum, 
																BlockNumber blocknum, BlockNumber* p_targetseg)
{
	return smgrsw[reln->smgr_which].smgr_getsegnumandrelativepagenum(reln, 
																																	 forknum,
																																	 blocknum,
																																	 p_targetseg);
							
}

/*
 * smgrwritevi() -- write the version index
 */
int
smgrwritevi(SMgrRelation reln, ForkNumber forknum, BlockNumber segnum,
						char *buffer, int size_vi, off_t seekpos)
{
	return smgrsw[reln->smgr_which].smgr_writevi(reln, forknum, segnum,
																							 buffer, size_vi, seekpos);
}

/*
 * smgrwritevi() -- write the version index
 */
int
smgrwritevisync(SMgrRelation reln, ForkNumber forknum, BlockNumber segnum)
{
	return smgrsw[reln->smgr_which].smgr_writevisync(reln, forknum, segnum);
}


/*
 * smgrgetvifd() -- get raw file descriptor of relation version index 
 */
int
smgrgetvifd(SMgrRelation reln, ForkNumber forknum, BlockNumber segnum)
{
	return smgrsw[reln->smgr_which].smgr_getvifd(reln, forknum, segnum);
}

/*
 * smgrgetvifdidx() -- get raw file descriptor of relation version index 
 */
int
smgrgetvifdidx(SMgrRelation reln, ForkNumber forknum, BlockNumber segnum)
{
	return smgrsw[reln->smgr_which].smgr_getvifdidx(reln, forknum, segnum);
}

/*
 * smgrgetsegfd() -- get raw file descriptor of relation segment
 */
int
smgrgetsegfd(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum)
{
	return smgrsw[reln->smgr_which].smgr_getsegfd(reln, forknum, blocknum);
}

/*
 * smgrsyncseg() -- write the version index
 */
int
smgrsyncseg(SMgrRelation reln, ForkNumber forknum, int fd)
{
	return smgrsw[reln->smgr_which].smgr_syncseg(reln, forknum, fd);
}
#endif

#ifdef SMARTSSD_DBUF

/*
 * ndpsmgrsetdirtybufid -- set dirty buf IDs to list
 */
void
ndpsmgrsetdirtybufid(Buffer buf)
{
	DBufTag dbufkey;
	LWLock *alloc_lock, * list_content_lock;
	uint hashcode;
	volatile DirtyBufIDList dbuflist;
	DBufLookupEnt * result;

  if (!IsTargetBuffer(GetBufferDescriptor(buf - 1)->tag.rnode.relNode))
		return;

	Assert(BufferIsPinned2(buf) || 
		pg_atomic_read_u32(&GetBufferDescriptor(buf - 1)->state) & BM_LOCKED);
	Assert(BufferIsValid(buf));
	Assert(LWLockHeldByMe(BufferDescriptorGetContentLock(
						  GetBufferDescriptor(buf - 1))));

	dbufkey.relid = GetBufferDescriptor(buf - 1)->tag.rnode.relNode;
	dbufkey.segno = GetBufferDescriptor(buf - 1)->tag.blockNum / RELSEG_SIZE;
	hashcode = NDPDBufHashCode(&dbufkey, SharedBufIDSetHash);
	alloc_lock = DBufChainAllocLock(hashcode);

	LWLockAcquire(alloc_lock, LW_SHARED);

	pg_memory_barrier();
	if (!(result = NDPHashLookup(&dbufkey,  hashcode, SharedBufIDSetHash)))
	{
		pg_memory_barrier();
		LWLockRelease(alloc_lock);

		LWLockAcquire(alloc_lock, LW_EXCLUSIVE);

		pg_memory_barrier();
		result = NDPHashLookup(&dbufkey, hashcode, SharedBufIDSetHash);
		if (!result) 
		{
			result = NDPHashInsert(&dbufkey, hashcode, SharedBufIDSetHash);
			LWLockRelease(alloc_lock);
			LWLockAcquire(alloc_lock, LW_SHARED);
		}
		else
		{
			LWLockRelease(alloc_lock);
			LWLockAcquire(alloc_lock, LW_SHARED);
		}
	}

	dbuflist = &(result->dbufidlist);
	list_content_lock = DBufDListLock(dbuflist->global_lock_number);
	LWLockAcquire(list_content_lock, LW_EXCLUSIVE);

#ifdef SMARTSSD_DBUF_DEBUG
	NDPSetDirtyBufID(dbuflist, buf, dbufkey.relid, dbufkey.segno);
#else
	NDPSetDirtyBufID(dbuflist, buf);
#endif

	pg_memory_barrier();
	LWLockRelease(list_content_lock);
	LWLockRelease(alloc_lock);
}	

/*
 * ndpsmgrpopdirtybufid -- pop dirty buf IDs from list
 */
void 
ndpsmgrpopdirtybufid(Buffer buf)
{
	DBufTag dbufkey;
	LWLock *alloc_lock, * list_content_lock;
	uint hashcode;
	volatile DirtyBufIDList dbuflist;
	DBufLookupEnt * result;

  if (!IsTargetBuffer(GetBufferDescriptor(buf - 1)->tag.rnode.relNode))
		return;

	Assert(BufferIsPinned2(buf));
	Assert(BufferIsValid(buf));
	Assert(LWLockHeldByMeInMode(BufferDescriptorGetContentLock(
			GetBufferDescriptor(buf - 1)), LW_SHARED));

	dbufkey.relid = GetBufferDescriptor(buf - 1)->tag.rnode.relNode;
	dbufkey.segno = GetBufferDescriptor(buf - 1)->tag.blockNum / RELSEG_SIZE;
	hashcode = NDPDBufHashCode(&dbufkey, SharedBufIDSetHash);
	alloc_lock = DBufChainAllocLock(hashcode);

	LWLockAcquire(alloc_lock, LW_SHARED);

	pg_memory_barrier();
	if (!(result = NDPHashLookup(&dbufkey,  hashcode, SharedBufIDSetHash)))
	{
		pg_memory_barrier();
		LWLockRelease(alloc_lock);
			
		Assert(0);

		return;
	}

	dbuflist = &(result->dbufidlist);
	
	list_content_lock = DBufDListLock(dbuflist->global_lock_number);
	LWLockAcquire(list_content_lock, LW_EXCLUSIVE);

	if (dbuflist->num_elem)
	{
#ifdef SMARTSSD_DBUF_DEBUG
		NDPPopDirtyBufID(dbuflist, buf, dbufkey.relid, dbufkey.segno);
#else
		NDPPopDirtyBufID(dbuflist, buf);
#endif
	}

	pg_memory_barrier();
	LWLockRelease(list_content_lock);
	LWLockRelease(alloc_lock);
}	

/*
 * ndpsmgrdetachdirtybufid -- detach dirty buf IDs from list
 */
FlushDirtyBufIDList
ndpsmgrdetachdirtybufid(Oid relid, uint32 segno)
{
	DBufTag dbufkey;
	LWLock *alloc_lock, * list_content_lock;
	uint hashcode;
	volatile DirtyBufIDList dbuflist;
	DBufLookupEnt * result;
	FlushDirtyBufIDList ret_list;

	dbufkey.relid = relid;
	dbufkey.segno = segno;
	hashcode = NDPDBufHashCode(&dbufkey, SharedBufIDSetHash);
	alloc_lock = DBufChainAllocLock(hashcode);

	LWLockAcquire(alloc_lock, LW_SHARED);

	pg_memory_barrier();
	if (!(result = NDPHashLookup(&dbufkey,  hashcode, SharedBufIDSetHash)))
	{
		pg_memory_barrier();
		LWLockRelease(alloc_lock);
		return NULL;
	}

	dbuflist = &(result->dbufidlist);
	list_content_lock = DBufDListLock(dbuflist->global_lock_number);

#ifdef SMARTSSD_DBUF_DEBUG
	fprintf(stderr, "Rel[%u][%u] flush with Lock[%d] %p\n", relid, segno,
					dbuflist->global_lock_number, list_content_lock);
#endif
	LWLockAcquire(list_content_lock, LW_EXCLUSIVE);

	pg_memory_barrier();
	ret_list = NDPDetachDirtyBufIDs(dbuflist, relid, segno,
									   list_content_lock);

	pg_memory_barrier();
	LWLockRelease(alloc_lock);

	return ret_list;
}

/*
 * ndpsmgrreleasedirtybufid -- set dirty buf IDs to list
 */
void
ndpsmgrreleasedirtybufid(Oid relid, uint32 max_segno, bool remove)
{
	DBufTag dbufkey;
	LWLock *alloc_lock, * list_content_lock;
	uint hashcode;
	volatile DirtyBufIDList dbuflist;
	DBufLookupEnt * result;
	uint32 segno;

	dbufkey.relid = relid;

	for (segno = 0 ; segno < max_segno ; segno++)
	{
		dbufkey.segno = segno;
		hashcode = NDPDBufHashCode(&dbufkey, SharedBufIDSetHash);
		alloc_lock = DBufChainAllocLock(hashcode);

		LWLockAcquire(alloc_lock, LW_SHARED);

		pg_memory_barrier();
		if (!(result = NDPHashLookup(&dbufkey,  hashcode, SharedBufIDSetHash)))
		{
			pg_memory_barrier();
			LWLockRelease(alloc_lock);
			continue;
		}
		LWLockRelease(alloc_lock);

		LWLockAcquire(alloc_lock, LW_EXCLUSIVE);
		
		pg_memory_barrier();
		if (!(result = NDPHashLookup(&dbufkey,  hashcode, SharedBufIDSetHash)))
		{
			pg_memory_barrier();
			LWLockRelease(alloc_lock);
			continue;
		}

		dbuflist = &(result->dbufidlist);
		list_content_lock = DBufDListLock(dbuflist->global_lock_number);

		LWLockAcquire(list_content_lock, LW_EXCLUSIVE);

		NDPReleaseDirtyBufID(dbuflist, relid, segno);
		pg_memory_barrier();
		NDPHashDelete(&dbufkey, hashcode, SharedBufIDSetHash);

		LWLockRelease(list_content_lock);
		LWLockRelease(alloc_lock);
	}
}	
#endif

#ifdef SMARTSSD
/*
 * smgrgetsegfd() -- get raw file descriptor of relation segment
 */
int
smgrgetsegfd(SMgrRelation reln, ForkNumber forknum, BlockNumber segno)
{
	return smgrsw[reln->smgr_which].smgr_getsegfd(reln, forknum, segno);
}
#endif // ifdef SMARTSSD

#ifdef WSUL_CTID_TEST
/* smgropenCTIDs() -- open a CTID file for the relation */
int smgropenCTIDs(SMgrRelation reln) {
	return smgrsw[reln->smgr_which].smgr_openCTIDs(reln);
}

/* smgrwriteCTID() -- write a CTID to a file */
size_t 
smgrwriteCTID(SMgrRelation reln, BlockNumber blocknum, 
		char *buffer, uint32 len, off_t pos) {
	return smgrsw[reln->smgr_which].smgr_writeCTID(reln, blocknum, 
			buffer, len, pos);
}

/* smgrcloseCTID() -- update count on the header of a CTIDs file 
	 and sync the file */
int smgrcloseCTIDs(SMgrRelation reln) {
	return smgrsw[reln->smgr_which].smgr_closeCTIDs(reln);
}
#endif //WSUL_CTID_TEST
