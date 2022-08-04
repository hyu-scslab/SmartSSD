/*-------------------------------------------------------------------------
 *
 * vimanger.h
 *	  Exports from postmaster/vimanger.c.
 *
 * (version_index_manager = vimanager = vim)
 *
 * src/include/postmaster/vimanager.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef VIMANAGER_H
#define VIMANAGER_H

#define XMINMAP_MAX_RELATIONS	(20)
#define XMINMAP_MAX_SEGMENTS	(100)
#define XMINMAP_MAX_GROUPS		(300)
#define XMINMAP_PAGES_PER_GROUP	(500)

/*
 * The main vimanager shmem struct.
 */
typedef struct
{
	pid_t			 vimanager_pid;  /* PID (0 if not started) */
	pg_atomic_uint64 page_allocator; /* Helper for page allocation */

	Oid rel_ids[XMINMAP_MAX_RELATIONS]; /* [16497, 16492 ...] */
	TransactionId xminmap[XMINMAP_MAX_RELATIONS][XMINMAP_MAX_SEGMENTS][XMINMAP_MAX_GROUPS];

	/* Stats */
} ViManagerShmemStruct;

extern ViManagerShmemStruct *ViManagerShmem;

// extern dsa_area *vi_manager_dsa_area;

extern void ViManagerMain(void) pg_attribute_noreturn();

/* shared memory */
extern Size ViManagerShmemSize(void);
extern void ViManagerShmemInit(void);

extern void ViSetMinXminPerGroup(Oid rel_id, Oid seg_id, uint32 page_id,
								 TransactionId xmin);
extern TransactionId ViGetMinXminPerGroup(Oid rel_id, Oid seg_id, uint32 page_id);

#endif /* VIMANAGER_H */
