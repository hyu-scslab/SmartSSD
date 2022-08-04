#ifdef SMARTSSD

#ifndef NDP_VERSION_ENTRY_CACHE_H
#define NDP_VERSION_ENTRY_CACHE_H

#include "storage/ndp_support.h"
#include "storage/relfilenode.h"
#include "utils/relcache.h"

#include "utils/dsa.h"

/* Meta-data start point */
#define METADATAPTR 0

/* offset of number of flushed version index */
#define OFFNUMFLUSHEDVI (METADATAPTR)

/* area size of number of flushed version index */
#define SIZENUMFLUSHEDVI (sizeof(uint32_t))

/* Data start point */
#define VISTARTPTR (METADATAPTR + SIZENUMFLUSHEDVI)

/* size of tyoe 1 version index */
#define SIZEOFVITYPE1 (sizeof(uint32_t) + sizeof(uint16_t))

/* multiplier of segment */
#define VICACHEMULTIPLIER 1

/* size of one segment array */
#define SIZEOFVICACHE (BLCKSZ * (SIZEOFVITYPE1) * VICACHEMULTIPLIER)

/* max number of in-memory version index array for type1 */
#define MAXNUMCACHEDVI (SIZEOFVICACHE / (SIZEOFVITYPE1))

/* direct pointer number of version index arr per segment for type1 */
#define NDIRECTSEGPTR 9 

/* indirect pointer number of version index array per segment for type1 */
#define NINDIRECTSEGPTR 20

#define VIAllocLock() (NULL)

/*** structure of version index ***/
typedef struct VersionIndex
{
	/* Record offset in segment */
	uint32_t off_in_seg;	
	/* length of record */
	uint16_t len_of_rec;	
} VersionIndex;

/* data type of showing if setting version index is finished. */ 
typedef uint8_t completed_flag;

/*** structure of version index array set per segment ***/
typedef struct VersionIndexArrPerSegmentData
{
	/* number of version index */
	pg_atomic_uint64 num_of_elem;

	/* number of cached version index */
	pg_atomic_uint64 num_of_cached_elem;

	/* number of flushed version index */
	pg_atomic_uint64 num_of_flushed_elem;
	
	/* version index array */
	char version_index_arr[SIZEOFVICACHE];

	/* completed flag array */
	completed_flag completed_flag_arr[MAXNUMCACHEDVI];
} VersionIndexArrPerSegmentData;

typedef struct VersionIndexArrPerSegmentData* VersionIndexArrPerSegment;

/*** structure of version index array set pointer indirection array ***/
typedef struct VersionIndexIndirectArrPerSegmentData
{
	/* version index array pointer of indirect block */
	pg_atomic_uint64 version_index_set_per_segment[NDIRECTSEGPTR + 2];
} VersionIndexIndirectArrPerSegmentData;

typedef struct VersionIndexIndirectArrPerSegmentData* 
							 VersionIndexIndirectArrPerSegment;

/*** structure of version index cache descriptor ***/
typedef struct VersionIndexCacheData
{
	/* relation id of cached version index */
	RelFileNode rnode;
	/* flag to show if version index cache slot is used */
	int used;
	/* flag to show if version indexes of this descriptor is flushing */
	pg_atomic_uint32 flushing;
	/* lowest segment id in version index cache data (maybe need for type2) */
	uint32_t lowest_segment_id;
	/* version index type of this version index cache */
	vi_type type;
	/* pointers of version index set */
	pg_atomic_uint64 version_index_set_per_segment[NDIRECTSEGPTR + 1];
  /* size of header per relation */
  uint8_t header_size;
} VersionIndexCacheData;

typedef struct VersionIndexCacheData* VersionIndexCache;

int VersionIndexSet(Relation rel, BlockNumber blocknum, uint16_t lp_off, 
										uint16_t lp_len, uint8_t hoff);
int VersionIndexFlush(Relation rel);

void VersionIndexAttachDsa(void);
void VersionIndexDetachDsa(void);
void VersionIndexInit(void);
Size VersionIndexShmemSize(void);
int CheckPointVersionIndex(void);
uint8_t GetHeaderSize(Relation rel);

#ifdef SMARTSSD_VI_DEBUG
int PrintOffAndLen(Relation rel, uint32_t off, uint16_t len, uint32_t block_no);

#endif

#endif // #ifndef NDP_VERSION_ENTRY_CACHE_H
#endif // #ifdef SMARTSSD
