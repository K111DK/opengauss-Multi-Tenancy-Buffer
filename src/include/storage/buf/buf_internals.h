/* -------------------------------------------------------------------------
 *
 * buf_internals.h
 *	  Internal definitions for buffer manager and the buffer replacement
 *	  strategy.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/buf/buf_internals.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef BUFMGR_INTERNALS_H
#define BUFMGR_INTERNALS_H

#include "storage/buf/buf.h"
#include "storage/buf/bufmgr.h"
#include "storage/latch.h"
#include "storage/lock/lwlock.h"
#include "storage/shmem.h"
#include "storage/smgr/smgr.h"
#include "storage/spin.h"
#include "utils/relcache.h"
#include "utils/atomic.h"
#include "access/xlogdefs.h"

/*
 * Buffer state is a single 32-bit variable where following data is combined.
 *
 * - 18 bits refcount
 * - 4 bits usage count
 * - 10 bits of flags
 *
 * Combining these values allows to perform some operations without locking
 * the buffer header, by modifying them together with a CAS loop.
 *
 * The definition of buffer state components is below.
 */
#define BUF_REFCOUNT_ONE 1
#define BUF_REFCOUNT_MASK ((1U << 16) - 1)
#define BUF_USAGECOUNT_MASK 0x003C0000U
#define BUF_USAGECOUNT_ONE (1U << 18)
#define BUF_USAGECOUNT_SHIFT 18
#define BUF_FLAG_MASK 0xFFC00000U

/* Get refcount and usagecount from buffer state */
#define BUF_STATE_GET_REFCOUNT(state) ((state)&BUF_REFCOUNT_MASK)
#define BUF_STATE_GET_USAGECOUNT(state) (((state)&BUF_USAGECOUNT_MASK) >> BUF_USAGECOUNT_SHIFT)

/*
 * Flags for buffer descriptors
 *
 * Note: TAG_VALID essentially means that there is a buffer hashtable
 * entry associated with the buffer's tag.
 */
#define BM_IN_MIGRATE (1U << 16)        /* buffer is migrating */
#define BM_IS_META (1U << 17)
#define BM_LOCKED (1U << 22)            /* buffer header is locked */
#define BM_DIRTY (1U << 23)             /* data needs writing */
#define BM_VALID (1U << 24)             /* data is valid */
#define BM_TAG_VALID (1U << 25)         /* tag is assigned */
#define BM_IO_IN_PROGRESS (1U << 26)    /* read or write in progress */
#define BM_IO_ERROR (1U << 27)          /* previous I/O failed */
#define BM_JUST_DIRTIED (1U << 28)      /* dirtied since write started */
#define BM_PIN_COUNT_WAITER (1U << 29)  /* have waiter for sole pin */
#define BM_CHECKPOINT_NEEDED (1U << 30) /* must write for checkpoint */
#define BM_PERMANENT                      \
    (1U << 31) /* permanent relation (not \
                * unlogged, or init fork) ) */
/*
 * The maximum allowed value of usage_count represents a tradeoff between
 * accuracy and speed of the clock-sweep buffer management algorithm.  A
 * large value (comparable to NBuffers) would approximate LRU semantics.
 * But it can take as many as BM_MAX_USAGE_COUNT+1 complete cycles of
 * clock sweeps to find a free buffer, so in practice we don't want the
 * value to be very large.
 */
#define BM_MAX_USAGE_COUNT 15

/*
 * Buffer tag identifies which disk block the buffer contains.
 *
 * Note: the BufferTag data must be sufficient to determine where to write the
 * block, without reference to pg_class or pg_tablespace entries.  It's
 * possible that the backend flushing the buffer doesn't even believe the
 * relation is visible yet (its xact may have started before the xact that
 * created the rel).  The storage manager must be able to cope anyway.
 *
 * Note: if there's any pad bytes in the struct, INIT_BUFFERTAG will have
 * to be fixed to zero them, since this struct is used as a hash key.
 */
typedef struct buftag {
    RelFileNode rnode; /* physical relation identifier */
    ForkNumber forkNum;
    BlockNumber blockNum; /* blknum relative to begin of reln */
} BufferTag;

typedef struct buftagnocompress {
    RelFileNodeV2 rnode;
    ForkNumber forkNum;
    BlockNumber blockNum; /* blknum relative to begin of reln */
} BufferTagSecondVer;


typedef struct buftagnohbkt {
    RelFileNodeOld rnode; /* physical relation identifier */
    ForkNumber forkNum;
    BlockNumber blockNum; /* blknum relative to begin of reln */
} BufferTagFirstVer;

/* entry for buffer lookup hashtable */
typedef struct {
    BufferTag key; /* Tag of a disk page */
    int id;        /* Associated buffer ID */
} BufferLookupEnt;

#define CLEAR_BUFFERTAG(a)               \
    ((a).rnode.spcNode = InvalidOid,     \
        (a).rnode.dbNode = InvalidOid,   \
        (a).rnode.relNode = InvalidOid,  \
        (a).rnode.bucketNode = -1,\
        (a).rnode.opt = DefaultFileNodeOpt, \
        (a).forkNum = InvalidForkNumber, \
        (a).blockNum = InvalidBlockNumber)

#define INIT_BUFFERTAG(a, xx_rnode, xx_forkNum, xx_blockNum) \
    ((a).rnode = (xx_rnode), (a).forkNum = (xx_forkNum), (a).blockNum = (xx_blockNum))

#define BUFFERTAGS_EQUAL(a, b) \
    (RelFileNodeEquals((a).rnode, (b).rnode) && (a).blockNum == (b).blockNum && (a).forkNum == (b).forkNum)

#define BUFFERTAGS_PTR_EQUAL(a, b) \
    (RelFileNodeEquals((a)->rnode, (b)->rnode) && (a)->blockNum == (b)->blockNum && (a)->forkNum == (b)->forkNum)

#define BUFFERTAGS_PTR_SET(a, b)                 \
    ((a)->rnode.spcNode = (b)->rnode.spcNode,    \
        (a)->rnode.dbNode = (b)->rnode.dbNode,   \
        (a)->rnode.relNode = (b)->rnode.relNode, \
        (a)->rnode.bucketNode = (b)->rnode.bucketNode,\
        (a)->rnode.opt = (b)->rnode.opt,             \
        (a)->forkNum = (b)->forkNum,             \
        (a)->blockNum = (b)->blockNum)

/*
 * The shared buffer mapping table is partitioned to reduce contention.
 * To determine which partition lock a given tag requires, compute the tag's
 * hash code with BufTableHashCode(), then apply BufMappingPartitionLock().
 * NB: NUM_BUFFER_PARTITIONS must be a power of 2!
 */
#define BufTableHashPartition(hashcode) ((hashcode) % NUM_BUFFER_PARTITIONS)
#define BufMappingPartitionLock(hashcode) \
	(&t_thrd.shemem_ptr_cxt.mainLWLockArray[FirstBufMappingLock + \
		BufTableHashPartition(hashcode)].lock)
#define BufMappingPartitionLockByIndex(i) \
	(&t_thrd.shemem_ptr_cxt.mainLWLockArray[FirstBufMappingLock + (i)].lock)

/*
 *	BufferDesc -- shared descriptor/state data for a single shared buffer.
 *
 * Note: Buffer header lock (BM_LOCKED flag) must be held to examine or change
 * the tag, state or wait_backend_pid fields.  In general, buffer header lock
 * is a spinlock which is combined with flags, refcount and usagecount into
 * single atomic variable.  This layout allow us to do some operations in a
 * single atomic operation, without actually acquiring and releasing spinlock;
 * for instance, increase or decrease refcount.  buf_id field never changes
 * after initialization, so does not need locking. The LWLock can take care
 * of itself.  The buffer header lock is *not* used to control access to the
 * data in the buffer!
 *
 * It's assumed that nobody changes the state field while buffer header lock
 * is held.  Thus buffer header lock holder can do complex updates of the
 * state variable in single write, simultaneously with lock release (cleaning
 * BM_LOCKED flag).  On the other hand, updating of state without holding
 * buffer header lock is restricted to CAS, which insure that BM_LOCKED flag
 * is not set.  Atomic increment/decrement, OR/AND etc. are not allowed.
 *
 * An exception is that if we have the buffer pinned, its tag can't change
 * underneath us, so we can examine the tag without locking the buffer header.
 * Also, in places we do one-time reads of the flags without bothering to
 * lock the buffer header; this is generally for situations where we don't
 * expect the flag bit being tested to be changing.
 *
 * We can't physically remove items from a disk page if another backend has
 * the buffer pinned.  Hence, a backend may need to wait for all other pins
 * to go away.	This is signaled by storing its own PID into
 * wait_backend_pid and setting flag bit BM_PIN_COUNT_WAITER.  At present,
 * there can be only one such waiter per buffer.
 *
 * We use this same struct for local buffer headers, but the lock fields
 * are not used and not all of the flag bits are useful either.
 */
/* BufferMeta -- metadata for a buffer */
#define MAX_ACCESS_HISTORY 4
typedef struct BufferMeta {
    /* access hist  makesure tag on top */
    BufferTag tag;
    Buffer id;
    uint32 hashcode;
    uint32 tenant_oid;
    bool is_dirty;
    pg_atomic_uint32 pre_access_self;
    pg_atomic_uint32 pre_access_global;
    void* link;
} BufferMeta;
#define TENANT_NUM 8
typedef struct ShemmCxt{
    float tenant_fetures[TENANT_NUM][3];//cache size(float)  hit ratio(float)  traffic(float)
    int db_ready;
    float tenant_partitions[TENANT_NUM];
    float reward;
    int rl_ready;
} ShemmCxt;
typedef struct BufferDescExtra {
    /* Cached physical location for segment-page storage, used for xlog */
    uint8 seg_fileno;
    BlockNumber seg_blockno;

    /* below fields are used for incremental checkpoint */
    pg_atomic_uint64 rec_lsn;        /* recovery LSN */
    volatile uint64 dirty_queue_loc; /* actual loc of dirty page queue */
    bool encrypt; /* enable table's level data encryption */

    volatile uint64 lsn_on_disk;

    volatile bool aio_in_progress; /* indicate aio is in progress */
    BufferMeta meta;
} BufferDescExtra;
#define TWB_CANDIDATE 1U
#define TWB_BUFFERED (1U << 1)
#define TWB_IO_PENDING (1u << 2)
#define LRUC_CANDIDATE (1U << 3)
typedef struct BufferDesc {
    BufferTag tag; /* ID of page contained in buffer */
    int buf_id;    /* buffer's index number (from 0) */

    /* state of the tag, containing flags, refcount and usagecount */
    pg_atomic_uint32 state;

    ThreadId wait_backend_pid; /* backend PID of pin-count waiter */

    LWLock* io_in_progress_lock; /* to wait for I/O to complete */
    LWLock* content_lock;        /* to lock access to buffer contents */

    BufferDescExtra *extra;

    struct BufferDesc* next; /* link in freelist of buffers */
    struct BufferDesc* prev;
    uint32 tenantOid;
    pg_atomic_uint32 flush_state;
#ifdef USE_ASSERT_CHECKING
    volatile uint64 lsn_dirty;
#endif
} BufferDesc;

/*
 * Concurrent access to buffer headers has proven to be more efficient if
 * they're cache line aligned. So we force the start of the BufferDescriptors
 * array to be on a cache line boundary and force the elements to be cache
 * line sized.
 *
 * XXX: As this is primarily matters in highly concurrent workloads which
 * probably all are 64bit these days, and the space wastage would be a bit
 * more noticeable on 32bit systems, we don't force the stride to be cache
 * line sized on those. If somebody does actual performance testing, we can
 * reevaluate.
 *
 * Note that local buffer descriptors aren't forced to be aligned - as there's
 * no concurrent access to those it's unlikely to be beneficial.
 *
 * We use 64bit as the cache line size here, because that's the most common
 * size. Making it bigger would be a waste of memory. Even if running on a
 * platform with either 32 or 128 byte line sizes, it's good to align to
 * boundaries and avoid false sharing.
 */
#define BUFFERDESC_PAD_TO_SIZE (SIZEOF_VOID_P == 8 ? 64 : 1)

typedef union BufferDescPadded {
    BufferDesc bufferdesc;
    char pad[BUFFERDESC_PAD_TO_SIZE];
} BufferDescPadded;

#define GetBufferDescriptor(id) (&t_thrd.storage_cxt.BufferDescriptors[(id)].bufferdesc)
#define GetLocalBufferDescriptor(id) ((BufferDesc *)&u_sess->storage_cxt.LocalBufferDescriptors[(id)].bufferdesc)
#define BufferDescriptorGetBuffer(bdesc) ((bdesc)->buf_id + 1)

#define BufferGetBufferDescriptor(buffer)                          \
    (AssertMacro(BufferIsValid(buffer)), BufferIsLocal(buffer) ?   \
        (BufferDesc *)&u_sess->storage_cxt.LocalBufferDescriptors[-(buffer)-1].bufferdesc : \
        &t_thrd.storage_cxt.BufferDescriptors[(buffer)-1].bufferdesc)

#define BufferDescriptorGetContentLock(bdesc) (((bdesc)->content_lock))
/*
 * Functions for acquiring/releasing a shared buffer header's spinlock.  Do
 * not apply these to local buffers!
 */
extern uint32 LockBufHdr(BufferDesc* desc);

#ifdef ENABLE_THREAD_CHECK
extern "C" {
    void AnnotateHappensBefore(const char *f, int l, uintptr_t addr);
}
#define TsAnnotateHappensBefore(addr)      AnnotateHappensBefore(__FILE__, __LINE__, (uintptr_t)addr)
#else
#define TsAnnotateHappensBefore(addr)
#endif

#define UnlockBufHdr(desc, s)                                    \
    do {                                                         \
        /* ENABLE_THREAD_CHECK only, release semantic */         \
        TsAnnotateHappensBefore(&desc->state);                   \
        pg_write_barrier();                                      \
        pg_atomic_write_u32(&(desc)->state, (s) & (~BM_LOCKED)); \
    } while (0)

extern bool retryLockBufHdr(BufferDesc* desc, uint32* buf_state);
/*
 * The PendingWriteback & WritebackContext structure are used to keep
 * information about pending flush requests to be issued to the OS.
 */
typedef struct PendingWriteback {
    /* could store different types of pending flushes here */
    BufferTag tag;
} PendingWriteback;

/* struct forward declared in bufmgr.h */
typedef struct WritebackContext {
    /* pointer to the max number of writeback requests to coalesce */
    int* max_pending;

    /* current number of pending writeback requests */
    int nr_pending;

    /* pending requests */
    PendingWriteback pending_writebacks[WRITEBACK_MAX_PENDING_FLUSHES];
} WritebackContext;

/* in bufmgr.c */

/*
 * Structure to sort buffers per file on checkpoints.
 *
 * This structure is allocated per buffer in shared memory, so it should be
 * kept as small as possible.
 */
typedef struct CkptSortItem {
    Oid tsId;
    Oid relNode;
    int2 bucketNode;
    ForkNumber forkNum;
    BlockNumber blockNum;
    int buf_id;
} CkptSortItem;

/*
 * Internal routines: only called by bufmgr
 */
/* bufmgr.c */
extern void WritebackContextInit(WritebackContext* context, int* max_pending);
extern void IssuePendingWritebacks(WritebackContext* context);
extern void ScheduleBufferTagForWriteback(WritebackContext* context, BufferTag* tag);

/* freelist.c */
extern BufferDesc *StrategyGetBufferLRU(BufferAccessStrategy strategy, uint32 *buf_state);
extern BufferDesc *StrategyGetBuffer(BufferAccessStrategy strategy, uint32 *buf_state);

extern void StrategyFreeBuffer(volatile BufferDesc* buf);
extern bool StrategyRejectBuffer(BufferAccessStrategy strategy, BufferDesc* buf);

extern int StrategySyncStart(uint32* complete_passes, uint32* num_buf_alloc);
extern void StrategyNotifyBgWriter(int bgwprocno);

extern Size StrategyShmemSize(void);
extern void StrategyInitialize(bool init);

/* buf_table.c */
extern Size BufTableShmemSize(int size);
extern void InitBufTable(int size);
extern uint32 BufTableHashCode(BufferTag* tagPtr);
extern int BufTableLookup(BufferTag* tagPtr, uint32 hashcode);
extern int BufTableInsert(BufferTag* tagPtr, uint32 hashcode, int buf_id);
extern void BufTableDelete(BufferTag* tagPtr, uint32 hashcode);

/* localbuf.c */
extern void LocalPrefetchBuffer(SMgrRelation smgr, ForkNumber forkNum, BlockNumber blockNum);
extern BufferDesc* LocalBufferAlloc(SMgrRelation smgr, ForkNumber forkNum, BlockNumber blockNum, bool* foundPtr);
extern void MarkLocalBufferDirty(Buffer buffer);
extern void DropRelFileNodeLocalBuffers(const RelFileNode& rnode, ForkNumber forkNum, BlockNumber firstDelBlock);
extern void DropRelFileNodeAllLocalBuffers(const RelFileNode& rnode);
extern void AtEOXact_LocalBuffers(bool isCommit);
extern void update_wait_lockid(LWLock* lock);
extern char* PageDataEncryptForBuffer(Page page, BufferDesc *bufdesc, bool is_segbuf = false);
extern void FlushBuffer(void* buf, SMgrRelation reln, ReadBufferMethod flushmethod = WITH_NORMAL_CACHE, bool skipFsync = false);
extern void LocalBufferFlushAllBuffer();


#define MINIMAL_BUFFER_SIZE 256
#define ENABLE_MULTI_TENANTCY (g_instance.attr.attr_storage.enable_multi_tenant)
#define ENABLE_FIXED (!g_instance.attr.attr_storage.enable_mtrp)
#define ENABLE_UPDATE_WEIGHT (g_instance.attr.attr_storage.enable_update_weight)
#define ENABLE_UPDATE_STRUCT (g_instance.attr.attr_storage.enable_update_struct)
#define ENABLE_SAMPLING (g_instance.attr.attr_storage.enable_sampling)
#define ENABLE_HIST (g_instance.attr.attr_storage.enable_hist)
#define EXTRA_MEM_FACTOR (g_instance.attr.attr_storage.extra_mem_factor)
#define ENABLE_LOG (g_instance.attr.attr_storage.enable_log)
#define TENANT_NUM_PARAM (g_instance.attr.attr_storage.max_tenant)
#define MULTITENANT_RESET_ENABLE 1
#define HIT_IN_HIST -2
#define TENANT_NAME_LEN 32
#define MAX_TENANT 128
#define HIST_NAME "HIST"
#define NON_TENANT_NAME "NON_TENANT"
#define LOG_INTERVAL (g_instance.attr.attr_storage.log_interval)
enum BufferType{
    LRU = 0,
    CLOCK,
};
typedef struct buffer_node {
    BufferTag key;
    uint32 key_hash;
    int buffer_id;
    struct buffer_node* prev;
    struct buffer_node* next;
} buffer_node;
typedef struct tenant_buffer_cxt{
    //key
    char tenant_name[TENANT_NAME_LEN];
    
    //real buffer cxt
    pthread_mutex_t tenant_buffer_lock;
    BufferDesc real_dummy_head;
    BufferDesc real_dummy_tail;
    BufferDesc* sweep_hand;
    uint64 curr_real_size{0};
    uint64 max_real_size{0};

    //Buffer hit stat lock
    pthread_spinlock_t hit_stat_lock;
    pg_atomic_uint32 real_hits{0};
    pg_atomic_uint32 real_miss{0};
    pg_atomic_uint32 traffic{0};
    pg_atomic_uint32 over_max{0};
    float traffic_radio;
    pg_atomic_uint32 rehit_traffic{0};
    pg_atomic_uint32 rehit_dirty{0};
    pg_atomic_uint32 rehit_precentil_total{0};
    pg_atomic_uint32 rehit_precentil_total_dirty{0};
    /* Multi Tenant info */ 
    uint32 tenant_oid;
} tenant_buffer_cxt;
typedef struct tenant_info{   
    /* History list */
    pthread_mutex_t lockArray[NUM_BUFFER_PARTITIONS];

    /* Free list */
    pthread_spinlock_t free_list_lock;
    Buffer* buffer_pool;
    CandidateList buffer_list;
    /* <= NORMAL_SHARED_BUFFER_NUM - MINIMAL_BUFFER_NUM*/
    uint64 tenant_free_taken{0};
    uint64 non_tenant_free_taken{0};
    
    /* Back up buffer*/
    tenant_buffer_cxt non_tenant_buffer_cxt;
    
    /* Tenant cxt array */
    pthread_mutex_t tenant_stat_lock;/* Acquire while change weight */
    tenant_buffer_cxt tenant_buffer_cxt_array[MAX_TENANT];
    uint32 tenant_num{0};

    pg_atomic_uint32 adjust_done{1u};
    pg_atomic_uint32 total_traffic{0};
    pg_atomic_uint32 candidate_idx;
    pg_atomic_uint32 hit_traffic{0};
    pg_atomic_uint32 stall_traffic{0};
    pg_atomic_uint32 rehit_traffic{0};
    uint32 candidate_tenant[MAX_TENANT];
    pg_atomic_uint64 update_count{0};
} tenant_info;


#define ENABLE_TWB (g_instance.attr.attr_storage.enable_twb)
#define TWB_SIZE (g_instance.attr.attr_storage.twb_size)
typedef struct TWB {
    pg_atomic_uint32 total_fg_stall;
    pg_atomic_uint32 total_twb_flushed;
    uint32 twb_size;
    pg_atomic_uint32 twb_used;
    bool need_flushing;
    /* twb lock */
    pthread_spinlock_t twb_lock;
    
    /* store twb clean pages */
    CandidateList twb_free_list;

    /* store twb dirty pages */
    CandidateList twb_dirty_list;
    
    /* store the dirty buffer id that to be flushed */
    Buffer * dirty_buffer;

    /* twb buffer HTAB init */
    void * twb_hash_table;

}TWB;


typedef struct AccessHistory{
    /* History list */
    pthread_mutex_t lockArray[NUM_BUFFER_PARTITIONS];
    BufferMeta *cand_buf_list;
    volatile int cand_list_size;
    pg_atomic_uint64 head;
    pg_atomic_uint64 tail;
    HTAB * access_hist;
    pg_atomic_uint64 head_ts{0};
} AccessHistory;
extern AccessHistory g_access_history;
#define ENABLE_LEAF_QUICK_EVICT (g_instance.attr.attr_storage.enable_leaf_quick_evict)
#define ENABLE_LRUC (g_instance.attr.attr_storage.enable_lruc)
#define ENABLE_TAIL_SCAN (g_instance.attr.attr_storage.enable_tail_scan)
#define ENABLE_BUFFER_TYPE_SCAN (g_instance.attr.attr_storage.buffer_type_scan)
#define MAX_LRUC_SCAN_LEN (g_instance.attr.attr_storage.max_lruc_scan_len)
#define ENABLE_LRU (g_instance.attr.attr_storage.enable_lru)
#define INDEX_SKIP_FLUSH (g_instance.attr.attr_storage.skip_filter)

#define WRITE_LAT_US (g_instance.attr.attr_storage.write_lat_us)
#define ENABLE_SIM_LAT (g_instance.attr.attr_storage.enable_sim_lat)
#define LAT_RATIO (g_instance.attr.attr_storage.lat_ratio)
#define LAT_TYPE (g_instance.attr.attr_storage.lat_type) // 1 - both 2 - index 3 - data
typedef struct LRUC {
    /* store twb dirty pages */
    CandidateList lruc_dirty_list;
    
    /* store the dirty buffer id that to be flushed */
    Buffer * dirty_buffer;

    pg_atomic_uint64 scan_total;
    pg_atomic_uint64 trigger_scan;
    pg_atomic_uint64 got_clean;
} LRUC;


typedef struct shadow_lru {
    pg_atomic_uint32 free_list_idx;
    /* shadow lru lock */
    pthread_mutex_t lru_lock;
    BufferDesc lru_head;
    BufferDesc lru_tail;
    BufferDesc* lru_c_pointer;
} shadow_lru;


typedef struct buffer_write_info {
    /* flush */
    pg_atomic_uint64 fg_flushed;
    pg_atomic_uint64 bg_flushed;
    pg_atomic_uint64 index_flushed; /* Index split get page meets flush*/
    /* fetch */
    pg_atomic_uint64 total_fetch; /* Total BufferAlloc */

    /* miss */
    pg_atomic_uint64 total_miss;

    pg_atomic_uint32 global_timer;

    /* consecutive miss */
    shadow_lru shadow_lru_cxt;
} buffer_write_info;

#define BUF_HIST_LEN 8
typedef struct {
    int hist_idx;
    BufferDesc * desc_hist[BUF_HIST_LEN];
    BufferTag tag_hist[BUF_HIST_LEN];
    bool hit_hist[BUF_HIST_LEN];
    bool is_index[BUF_HIST_LEN];
    int access_cnt;
}miss_info;
extern THR_LOCAL miss_info g_miss_info;

/* */
extern buffer_write_info g_buffer_write_info;
extern tenant_info g_tenant_info;
extern TWB g_twb_info;
extern LRUC g_lruc_info;
extern BufferDesc *TenantStrategyGetBuffer(BufferAccessStrategy strategy, uint32* buf_state, tenant_buffer_cxt* buffer_cxt);
extern void show_tenant_status();
extern void XGB_evictor_main();
/* new */
extern void ThrdGetRefBufferIndex(tenant_buffer_cxt* buffer_cxt);
extern bool UpdateRefBuffer(uint32 access_hash, BufferTag *access_tag);
#endif /* BUFMGR_INTERNALS_H */
