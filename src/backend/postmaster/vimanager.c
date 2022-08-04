/*-------------------------------------------------------------------------
 *
 * vimanager.c
 *	  Version Index Manager Implementation
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/storage/vi/vimanager.c
 *
 *-------------------------------------------------------------------------
 */
#ifdef VERSION_INDEX

#include "postgres.h"

#include <signal.h>
#include <sys/time.h>
#include <unistd.h>

#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "utils/rel.h"
#include "access/xact.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/postmaster.h"
#include "postmaster/vimanager.h"
#include "storage/condition_variable.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "storage/smgr.h"
#include "storage/spin.h"
#include "storage/standby.h"
#include "storage/block.h"
#include "storage/itemid.h"
#include "storage/bufpage.h"
#include "storage/bufmgr.h"
#include "storage/vi.h"
#include "storage/vi_bufpage.h"
#include "storage/vi_buf.h"
#include "storage/vi_hash.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/timestamp.h"

/*
 * Version Index Manager
 *
 * version_index_manager = vimanager = vim
 */

/*
 * GUC parameters
 */
int ViManagerDelay = 1000; /* milli-seconds */

ViManagerShmemStruct *ViManagerShmem = NULL;

/*
 * Flags set by interrupt handlers for later service in the main loop.
 */
static volatile sig_atomic_t got_SIGHUP			= false;
static volatile sig_atomic_t shutdown_requested = false;

/* Signal handlers */

static void vimanager_quickdie(SIGNAL_ARGS);
static void ViManagerSigHupHandler(SIGNAL_ARGS);
static void ReqShutdownHandler(SIGNAL_ARGS);
static void vimanager_sigusr1_handler(SIGNAL_ARGS);

/* Other static functions */
static void ViManagerInit(void);
static void ViManagerDestroy(void);
static void ViManagerFlushBuffers(void);
static void ViManagerFlushBuffer(ViBuffer idx);

/*
 * Main entry point for version index manager process
 *
 * This is invoked from AuxiliaryProcessMain, which has already created the
 * basic execution environment, but not enabled signals yet.
 */
void
ViManagerMain(void)
{
	sigjmp_buf	local_sigjmp_buf;
	MemoryContext vimanager_context;

	uint32 tic = 0;

	/*
	 * Properly accept or ignore signals the postmaster might send us.
	 *
	 * vimanager doesn't participate in ProcSignal signalling, but a SIGUSR1
	 * handler is still needed for latch wakeups.
	 */
	pqsignal(SIGHUP, ViManagerSigHupHandler); /* set flag to read config file */
	pqsignal(SIGINT, SIG_IGN);
	pqsignal(SIGTERM, ReqShutdownHandler); /* shutdown */
	pqsignal(SIGQUIT, vimanager_quickdie); /* hard crash time */
	pqsignal(SIGALRM, SIG_IGN);
	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, vimanager_sigusr1_handler);
	pqsignal(SIGUSR2, SIG_IGN);

	/*
	 * Reset some signals that are accepted by postmaster but not here
	 */
	pqsignal(SIGCHLD, SIG_DFL);

	/* We allow SIGQUIT (quickdie) at all times */
	sigdelset(&BlockSig, SIGQUIT);

	/*
	 * Create a memory context that we will do all our work in.  We do this so
	 * that we can reset the context during error recovery and thereby avoid
	 * possible memory leaks.  Formerly this code just ran in
	 * TopMemoryContext, but resetting that would be a really bad idea.
	 */
	vimanager_context = AllocSetContextCreate(
		TopMemoryContext, "Version Index Manager", ALLOCSET_DEFAULT_SIZES);
	MemoryContextSwitchTo(vimanager_context);

	/*
	 * If an exception is encountered, processing resumes here.
	 *
	 * See notes in postgres.c about the design of this coding.
	 */
	if (sigsetjmp(local_sigjmp_buf, 1) != 0)
	{
		/* Since not using PG_TRY, must reset error stack by hand */
		error_context_stack = NULL;

		/* Prevent interrupts while cleaning up */
		HOLD_INTERRUPTS();

		/* Report the error to the server log */
		EmitErrorReport();

		/*
		 * These operations are really just a minimal subset of
		 * AbortTransaction().  We don't have very many resources to worry
		 * about in bgwriter, but we do have LWLocks, buffers, and temp files.
		 */
		LWLockReleaseAll();
		ConditionVariableCancelSleep();
		pgstat_report_wait_end();
		AbortBufferIO();
		UnlockBuffers();
		ReleaseAuxProcessResources(false);
		AtEOXact_Buffers(false);
		AtEOXact_SMgr();
		AtEOXact_Files(false);
		AtEOXact_HashTables(false);

		/*
		 * Now return to normal top-level context and clear ErrorContext for
		 * next time.
		 */
		MemoryContextSwitchTo(vimanager_context);
		FlushErrorState();

		/* Flush any leaked data in the top-level context */
		MemoryContextResetAndDeleteChildren(vimanager_context);

		/* Now we can allow interrupts again */
		RESUME_INTERRUPTS();

		/*
		 * Sleep at least 1 second after any error.  A write error is likely
		 * to be repeated, and we don't want to be filling the error logs as
		 * fast as we can.
		 */
		pg_usleep(1000000L);

		/*
		 * Close all open files after any error.  This is helpful on Windows,
		 * where holding deleted files open causes various strange errors.
		 * It's not clear we need it elsewhere, but shouldn't hurt.
		 */
		smgrcloseall();
	}

	/* We can now handle ereport(ERROR) */
	PG_exception_stack = &local_sigjmp_buf;

	/*
	 * Unblock signals (they were blocked when the postmaster forked us)
	 */
	PG_SETMASK(&UnBlockSig);

	ViManagerInit();

	/*
	 * Loop forever
	 */
	for (;;)
	{
		/* Clear any already-pending wakeups */
		ResetLatch(MyLatch);

		if (got_SIGHUP)
		{
			got_SIGHUP = false;
			ProcessConfigFile(PGC_SIGHUP);
		}
		if (shutdown_requested)
		{
			/*
			 * From here on, elog(ERROR) should end with exit(1), not send
			 * control back to the sigsetjmp block above
			 */
			ExitOnAnyError = true;
			/* Normal exit from the bgwriter is here */
			proc_exit(0); /* done */
		}

		// ereport(LOG, (errmsg("[ViManager] tic: %u", tic)));

		/* Increment tic for each round */
		++tic;

		/*
		 * Sleep until we are signaled or VersionIndexManagerDelay has elapsed.
		 *
		 * Note: the feedback control loop in BgBufferSync() expects that we
		 * will call it every BgWriterDelay msec.  While it's not critical for
		 * correctness that that be exact, the feedback loop might misbehave
		 * if we stray too far from that.  Hence, avoid loading this process
		 * down with latch events that are likely to happen frequently during
		 * normal operation.
		 */
		WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
				  ViManagerDelay, WAIT_EVENT_VERSION_INDEX_MANAGER_MAIN);
	}
}

/*
 * Compute space needed for version index manager related shared memory
 */
Size
ViManagerShmemSize(void)
{
	Size size = 0;

	size = add_size(size, sizeof(ViManagerShmemStruct));

	size = add_size(size, ViBufferShmemSize());

	return size;
}

/*
 * Allocate and initialize version index manager related shared memory
 */
void
ViManagerShmemInit(void)
{
	Size size = ViManagerShmemSize();

	bool found;

	/*
	 * Create or attach to the shared memory state, including hash table
	 */
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	ViManagerShmem = (ViManagerShmemStruct *)ShmemInitStruct(
		"ViManager Data", sizeof(ViManagerShmemStruct), &found);

	if (!found)
	{
		/*
		 * First time through, so initialize
		 */
		MemSet(ViManagerShmem, 0, size);
		Assert(ViManagerShmem != NULL);

		pg_atomic_init_u64(&ViManagerShmem->page_allocator, 0);
	}

	/*
	 * Initialize buffer
	 */
	InitViBufferPool();

	LWLockRelease(AddinShmemInitLock);
}

void
ViSetMinXminPerGroup(Oid rel_id,
					 Oid seg_id,
					 ViPageId page_id,
					 TransactionId xmin)
{
	int group_id = page_id / XMINMAP_PAGES_PER_GROUP;
	int idx;

	Assert(seg_id < XMINMAP_MAX_SEGMENTS);
	Assert(group_id < XMINMAP_MAX_GROUPS);

	for (idx = 0; idx < XMINMAP_MAX_RELATIONS; idx++)
	{
		if (ViManagerShmem->rel_ids[idx] == rel_id)
		{
			break;
		}
		else if (ViManagerShmem->rel_ids[idx] == 0)
		{
			ViManagerShmem->rel_ids[idx] = rel_id;
			break;
		}
	}

	Assert(idx < XMINMAP_MAX_RELATIONS);

	/* Protected by meta page wise exclusive lock */
	if ((ViManagerShmem->xminmap[idx][seg_id][group_id] == 0) ||
			(xmin < ViManagerShmem->xminmap[idx][seg_id][group_id]))
	{
		ViManagerShmem->xminmap[idx][seg_id][group_id] = xmin;
	}
}

TransactionId
ViGetMinXminPerGroup(Oid rel_id,
					 Oid seg_id,
					 ViPageId page_id)
{
	int group_id = page_id / XMINMAP_PAGES_PER_GROUP;
	int idx;

	Assert(seg_id < XMINMAP_MAX_SEGMENTS);
	Assert(group_id < XMINMAP_MAX_GROUPS);

	for (idx = 0; idx < XMINMAP_MAX_RELATIONS; idx++)
	{
		if (ViManagerShmem->rel_ids[idx] == rel_id)
		{
			break;
		}
		else if (ViManagerShmem->rel_ids[idx] == 0)
		{
			ViManagerShmem->rel_ids[idx] = rel_id;
			break;
		}
	}

	Assert(idx < XMINMAP_MAX_RELATIONS);

	/* Optimization, if lucky: read recent value, else: read old minimum */
	return ViManagerShmem->xminmap[idx][seg_id][group_id];
}

/*
 * Initialize any necessary resource for version index manager
 */
static void
ViManagerInit(void)
{
	Assert(sizeof(ViPageData) == VI_PAGE_SIZE);
	Assert(sizeof(ViPageHeaderData) == VI_PAGE_HEADER_SIZE);
	Assert(VI_PER_PAGE == VI_NUM_SLOTS);

	/* SmartSSD shows best performance on 64 byte read */
	Assert(sizeof(CtidChunkData) == SmartSSDChunkSize);
	Assert(sizeof(CtidData) == 9);

	Assert(sizeof(ViPerSegmentMetaData) <= VI_PAGE_SIZE);
}

/*
 * Destroy any remaning resource for version index manager
 */
static void
ViManagerDestroy(void)
{
	ViManagerFlushBuffers();
}

/*
 * Flush the dirty version index buffers
 */
static void
ViManagerFlushBuffers(void)
{
	int i;

	for (i = 0; i < NViBuffers; i++)
	{
		ViManagerFlushBuffer(i);
	}
}

/*
 * Internal operation of flushing a single dirty index buffer by its index
 */
static void
ViManagerFlushBuffer(ViBuffer idx)
{
	ViBufferDesc* desc;

	desc = GetViBufferDescriptor(idx);
	if (desc->is_dirty)
	{
		Assert(pg_atomic_read_u32(&desc->refcnt) == 0);
		ViWritePageExternal(&desc->tag, idx);
	}
}

/* --------------------------------
 *		signal handler routines
 * --------------------------------
 */

/*
 * vim_quickdie() occurs when signalled SIGQUIT by the postmaster.
 *
 * Some backend has bought the farm,
 * so we need to stop what we're doing and exit.
 */
static void vimanager_quickdie(SIGNAL_ARGS)
{
	ViManagerDestroy();

	/*
	 * We DO NOT want to run proc_exit() or atexit() callbacks -- we're here
	 * because shared memory may be corrupted, so we don't want to try to
	 * clean up our transaction.  Just nail the windows shut and get out of
	 * town.  The callbacks wouldn't be safe to run from a signal handler,
	 * anyway.
	 *
	 * Note we do _exit(2) not _exit(0).  This is to force the postmaster into
	 * a system reset cycle if someone sends a manual SIGQUIT to a random
	 * backend.  This is necessary precisely because we don't clean up our
	 * shared memory state.  (The "dead man switch" mechanism in pmsignal.c
	 * should ensure the postmaster sees this as a crash, too, but no harm in
	 * being doubly sure.)
	 */
	_exit(2);
}

/* SIGHUP: set flag to re-read config file at next convenient time */
static void ViManagerSigHupHandler(SIGNAL_ARGS)
{
	int save_errno = errno;

	got_SIGHUP = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

/* SIGTERM: set flag to shutdown and exit */
static void ReqShutdownHandler(SIGNAL_ARGS)
{
	int save_errno = errno;

	shutdown_requested = true;
	SetLatch(MyLatch);

	errno = save_errno;

	ViManagerDestroy();
}

/* SIGUSR1: used for latch wakeups */
static void vimanager_sigusr1_handler(SIGNAL_ARGS)
{
	int save_errno = errno;

	latch_sigusr1_handler();

	errno = save_errno;
}

#endif /* VERSION_INDEX */
