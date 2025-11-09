/* -------------------------------------------------------------------------
 *
 * buf_init.cpp
 *	  buffer manager initialization routines
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/buffer/buf_init.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "gs_bbox.h"
#include "storage/buf/bufmgr.h"
#include "storage/buf/buf_internals.h"
#include "storage/nvm/nvm.h"
#include "storage/ipc.h"
#include "storage/cucache_mgr.h"
#include "pgxc/pgxc.h"
#include "postmaster/pagewriter.h"
#include "postmaster/bgwriter.h"
#include "utils/palloc.h"
#include "ddes/dms/ss_dms_bufmgr.h"
#include "ddes/dms/ss_common_attr.h"
const int PAGE_QUEUE_SLOT_MULTI_NBUFFERS = 5;
/*
 * Data Structures:
 *		buffers live in a freelist and a lookup data structure.
 *
 *
 * Buffer Lookup:
 *		Two important notes.  First, the buffer has to be
 *		available for lookup BEFORE an IO begins.  Otherwise
 *		a second process trying to read the buffer will
 *		allocate its own copy and the buffer pool will
 *		become inconsistent.
 *
 * Buffer Replacement:
 *		see freelist.c.  A buffer cannot be replaced while in
 *		use either by data manager or during IO.
 *
 *
 * Synchronization/Locking:
 *
 * IO_IN_PROGRESS -- this is a flag in the buffer descriptor.
 *		It must be set when an IO is initiated and cleared at
 *		the end of the IO.	It is there to make sure that one
 *		process doesn't start to use a buffer while another is
 *		faulting it in.  see WaitIO and related routines.
 *
 * refcount --	Counts the number of processes holding pins on a buffer.
 *		A buffer is pinned during IO and immediately after a BufferAlloc().
 *		Pins must be released before end of transaction.  For efficiency the
 *		shared refcount isn't increased if a individual backend pins a buffer
 *		multiple times. Check the PrivateRefCount infrastructure in bufmgr.c.
 */
/*
 * Initialize shared buffer pool
 *
 * This is called once during shared-memory initialization (either in the
 * postmaster, or in a standalone backend).
 */
static void buf_push(CandidateList *list, int buf_id)
{
    uint32 list_size = list->cand_list_size;
    uint32 tail_loc;

    pg_memory_barrier();
    volatile uint64 head = pg_atomic_read_u64(&list->head);
    pg_memory_barrier();
    volatile uint64 tail = pg_atomic_read_u64(&list->tail);

    if (unlikely(tail - head >= list_size)) {
        return;
    }
    tail_loc = tail % list_size;
    list->cand_buf_list[tail_loc] = buf_id;
    (void)pg_atomic_fetch_add_u64(&list->tail, 1);
}
void InitTenantPrivateCxt(){
    bool kill_db = false;
    bool is_tenant = u_sess && u_sess->proc_cxt.MyProcPort && u_sess->proc_cxt.MyProcPort->user_name;
    if(is_tenant){
        is_tenant = u_sess->proc_cxt.MyProcPort->user_name[0] == 't' || u_sess->proc_cxt.MyProcPort->user_name[0] == 'T';
    }
    tenant_buffer_cxt* thrd_tenant = NULL;
    const char* curr_thrd_name = is_tenant ? u_sess->proc_cxt.MyProcPort->user_name : NON_TENANT_NAME;
    if(is_tenant){
        uint32 tenant_id = (curr_thrd_name[1] - '0') * 100 + (curr_thrd_name[2] - '0') * 10 + (curr_thrd_name[3] - '0');
        Assert(tenant_id < MAX_TENANT);
        thrd_tenant = &g_tenant_info.tenant_buffer_cxt_array[tenant_id];
    }else{
        thrd_tenant = &g_tenant_info.non_tenant_buffer_cxt;
    }
    /* Attach thrd_tenant cxt */
    t_thrd.thrd_tenant_buffer_cxt = (void *)thrd_tenant;
}
void InitBufferPool(bool *found_descs){
    g_tenant_info.buffer_pool = (Buffer *)
    ShmemInitStruct("MultiTenantBuffers", NORMAL_SHARED_BUFFER_NUM * sizeof(Buffer), found_descs);
    if(!(*found_descs)){
        /* Only happen once */
        MemSet((char*)g_tenant_info.buffer_pool, 0, NORMAL_SHARED_BUFFER_NUM * sizeof(Buffer));
        INIT_CANDIDATE_LIST(g_tenant_info.buffer_list, g_tenant_info.buffer_pool, NORMAL_SHARED_BUFFER_NUM ,0 ,0);
        g_tenant_info.buffer_list.buf_id_start = 0;
        for(int buf_id = 0; buf_id < NvmBufferStartID; buf_id++){
            buf_push(&g_tenant_info.buffer_list, buf_id);
        }
    }
}
static void InitTenantBufferLock(bool first_init){
    if(first_init){
        pthread_spin_init(&g_tenant_info.free_list_lock, NULL);
        pthread_mutex_init(&g_tenant_info.tenant_stat_lock, NULL);
    }
}
void InitAllTenant(bool is_first){
    if(!is_first)
        return;
    /* Normal tenant */
    int tenant_num = TENANT_NUM_PARAM;
    g_tenant_info.tenant_num = TENANT_NUM_PARAM;
    uint32 i;
    uint32 total_buffer_num = NORMAL_SHARED_BUFFER_NUM - MINIMAL_BUFFER_SIZE;
    int * array = (int *)CACHELINEALIGN(ShmemInitStruct("Array Buffer Pool",
    (TENANT_NUM_PARAM + 1) * TOTAL_BUFFER_NUM * sizeof(int),
    &is_first));
    for(i = 0; i < tenant_num; i++){
        tenant_buffer_cxt* tenant_cxt = &g_tenant_info.tenant_buffer_cxt_array[i];
        tenant_cxt->tenant_oid = i;
        tenant_cxt->max_real_size = total_buffer_num / tenant_num;
        tenant_cxt->curr_real_size = 0;
        tenant_cxt->real_dummy_head.next = &tenant_cxt->real_dummy_tail;
        tenant_cxt->real_dummy_head.prev = NULL;
        tenant_cxt->real_dummy_tail.prev = &tenant_cxt->real_dummy_head;
        tenant_cxt->real_dummy_tail.next = NULL;
        tenant_cxt->victim_head.next = &tenant_cxt->victim_tail;
        tenant_cxt->victim_tail.prev = &tenant_cxt->victim_head;
        /* Tenant 's mutex */
        pthread_mutex_init(&tenant_cxt->victim_lock, NULL);
        pthread_mutex_init(&tenant_cxt->tenant_buffer_lock, NULL);
        pthread_spin_init(&tenant_cxt->hit_stat_lock, NULL);
        tenant_cxt->array_buffer_pool = &array[0 + i * TOTAL_BUFFER_NUM];
    }

    /* Non tenant */
    g_tenant_info.non_tenant_buffer_cxt.array_buffer_pool = &array[ i * TOTAL_BUFFER_NUM ];
    g_tenant_info.non_tenant_buffer_cxt.max_real_size = MINIMAL_BUFFER_SIZE;
    g_tenant_info.non_tenant_buffer_cxt.curr_real_size = 0;
    g_tenant_info.non_tenant_buffer_cxt.real_dummy_head.next = &g_tenant_info.non_tenant_buffer_cxt.real_dummy_tail;
    g_tenant_info.non_tenant_buffer_cxt.real_dummy_head.prev = NULL;
    g_tenant_info.non_tenant_buffer_cxt.real_dummy_tail.prev = &g_tenant_info.non_tenant_buffer_cxt.real_dummy_head;
    g_tenant_info.non_tenant_buffer_cxt.real_dummy_tail.next = NULL;
    /* Tenant 's mutex */
    pthread_mutex_init(&g_tenant_info.non_tenant_buffer_cxt.tenant_buffer_lock, NULL);
    pthread_spin_init(&g_tenant_info.non_tenant_buffer_cxt.hit_stat_lock, NULL);
}
void InitMultiTenantBufferPool(void){

    bool found_descs = false;  
    /* Free pool init */
    InitBufferPool(&found_descs);
    
    /* Lock only init once */
    InitTenantBufferLock(!found_descs);

    InitAllTenant(!found_descs);

    /* Init tenant buffer */
    InitTenantPrivateCxt();    
}
void TWB_init(){
    bool first;
    /* Basic info */
    pg_atomic_init_u32(&g_twb_info.total_fg_stall, 0);
    pg_atomic_init_u32(&g_twb_info.total_twb_flushed, 0);    
    g_twb_info.need_flushing = false;
    g_twb_info.twb_size = TWB_SIZE;
    pg_atomic_init_u32(&g_twb_info.twb_used, 0);

    /* Init twb flush array */
    bool found_twb_info = false;
    g_twb_info.dirty_buffer = (Buffer *)CACHELINEALIGN(
    ShmemInitStruct("TWB Dirty Buffer",
    TWB_SIZE * sizeof(Buffer) + PG_CACHE_LINE_SIZE, &found_twb_info));
    
    /* Init twb free list */
    Buffer * twb_free_buf_pool = (Buffer *)CACHELINEALIGN(
    ShmemInitStruct("twb free list", TWB_SIZE * sizeof(Buffer), &first));
    MemSet((char*)twb_free_buf_pool, 0, TWB_SIZE * sizeof(Buffer));
    INIT_CANDIDATE_LIST(g_twb_info.twb_free_list, twb_free_buf_pool, 
    TWB_SIZE, 0 ,0);
    for(int i = NORMAL_SHARED_BUFFER_NUM - TWB_SIZE; i < NORMAL_SHARED_BUFFER_NUM; i++) {
        buf_push(&g_twb_info.twb_free_list, i);
    }

    /* Init twb dirty list */
    Buffer * twb_dirty_buf_pool = (Buffer *)CACHELINEALIGN(
    ShmemInitStruct("twb dirty list", TWB_SIZE * sizeof(Buffer), &first));
    MemSet((char*)twb_dirty_buf_pool, 0, TWB_SIZE * sizeof(Buffer));
    INIT_CANDIDATE_LIST(g_twb_info.twb_dirty_list, twb_dirty_buf_pool,
    TWB_SIZE, 0 ,0);
    ereport(LOG,(errmsg("TWB init, size: %u", g_twb_info.twb_size)));
}
void LRUC_init(){ 
    /* Init lruc dirty list */
    bool first;
    Buffer * lruc_dirty_buf_pool = (Buffer *)CACHELINEALIGN(
    ShmemInitStruct("lru-c dirty list", TOTAL_BUFFER_NUM * sizeof(Buffer), &first));
    MemSet((char*)lruc_dirty_buf_pool, 0, TOTAL_BUFFER_NUM * sizeof(Buffer));
    INIT_CANDIDATE_LIST(g_lruc_info.lruc_dirty_list, lruc_dirty_buf_pool,
    TOTAL_BUFFER_NUM, 0 ,0);
    pg_atomic_init_u64(&g_lruc_info.scan_total, 0);
    pg_atomic_init_u64(&g_lruc_info.trigger_scan, 0);
    pg_atomic_init_u64(&g_lruc_info.got_clean, 0);
    ereport(LOG,(errmsg("LRUC init")));

}
void LRU_init(){
        
        pg_atomic_init_u64(&g_buffer_write_info.bg_flushed, 0);
        pg_atomic_init_u64(&g_buffer_write_info.fg_flushed, 0);
        pg_atomic_init_u64(&g_buffer_write_info.total_fetch, 0);
        pg_atomic_init_u64(&g_buffer_write_info.total_miss, 0);

        pthread_mutex_init(&g_buffer_write_info.shadow_lru_cxt.lru_lock, NULL);
        pg_atomic_init_u32(&g_buffer_write_info.shadow_lru_cxt.free_list_idx, 0);
        pg_atomic_init_u32(&g_buffer_write_info.global_timer, 0);
        
        /* LRU init */
        BufferDesc * head = &g_buffer_write_info.shadow_lru_cxt.lru_head;
        BufferDesc * tail = &g_buffer_write_info.shadow_lru_cxt.lru_tail;
        g_buffer_write_info.shadow_lru_cxt.lru_c_pointer = tail;
        head->prev = NULL;
        head->next = tail;
        tail->prev = head;
        tail->next = NULL;
        
}
void AccessHistoryInit(){
    bool first;
    g_access_history.cand_buf_list = (BufferMeta *)CACHELINEALIGN(
    ShmemInitStruct("Access History List", NORMAL_SHARED_BUFFER_NUM * sizeof(BufferMeta), &first));
    int i = 0;
    for(i; i < NUM_BUFFER_PARTITIONS; i++){
        pthread_mutex_init(&g_access_history.lockArray[i], NULL);
    }
    g_access_history.cand_list_size = NORMAL_SHARED_BUFFER_NUM;
    g_access_history.head = 0;
    g_access_history.tail = 0;
    MemSet((char*)g_access_history.cand_buf_list, 0, NORMAL_SHARED_BUFFER_NUM * sizeof(BufferMeta));

    HASHCTL hctl;
    int ret = memset_s(&hctl, sizeof(HASHCTL), 0, sizeof(HASHCTL));
    securec_check(ret, "\0", "\0");
    hctl.keysize = sizeof(BufferTag);//tag hash
    hctl.entrysize = sizeof(BufferMeta); // oid
    hctl.hash = tag_hash;
    hctl.num_partitions = NUM_BUFFER_PARTITIONS;
    g_access_history.access_hist = ShmemInitHash("access history hash",
    NORMAL_SHARED_BUFFER_NUM, NORMAL_SHARED_BUFFER_NUM, &hctl, HASH_ELEM | HASH_FUNCTION | HASH_PARTITION);
}
void InitBufferPool(void)
{
    bool found_bufs = false;
    bool found_descs = false;
    bool found_buf_ckpt = false;
    bool found_buf_extra = false;
    uint64 buffer_size;
    BufferDescExtra *extra = NULL;

    t_thrd.storage_cxt.BufferDescriptors = (BufferDescPadded *)CACHELINEALIGN(
        ShmemInitStruct("Buffer Descriptors",
                        TOTAL_BUFFER_NUM * sizeof(BufferDescPadded) + PG_CACHE_LINE_SIZE,
                        &found_descs));

    extra = (BufferDescExtra *)CACHELINEALIGN(
        ShmemInitStruct("Buffer Descriptors Extra",
                        TOTAL_BUFFER_NUM * sizeof(BufferDescExtra) + PG_CACHE_LINE_SIZE,
                        &found_buf_extra));
    if(!found_descs && ENABLE_LRU)
        LRU_init();
    
    if(!found_descs){
        AccessHistoryInit();
    }

    if(ENABLE_MULTI_TENANTCY){
        /* We make sure this won't exec twice */
        InitMultiTenantBufferPool();
    }


    /* Init candidate buffer list and candidate buffer free map */
    candidate_buf_init();

#ifdef __aarch64__
    buffer_size = (TOTAL_BUFFER_NUM - NVM_BUFFER_NUM) * (Size)BLCKSZ + PG_CACHE_LINE_SIZE;
    t_thrd.storage_cxt.BufferBlocks =
        (char *)CACHELINEALIGN(ShmemInitStruct("Buffer Blocks", buffer_size, &found_bufs));
#else
    if (ENABLE_DSS) {
        buffer_size = (uint64)((TOTAL_BUFFER_NUM - NVM_BUFFER_NUM) * (Size)BLCKSZ + ALIGNOF_BUFFER);
        t_thrd.storage_cxt.BufferBlocks =
            (char *)BUFFERALIGN(ShmemInitStruct("Buffer Blocks", buffer_size, &found_bufs));
    } else {
        buffer_size = (TOTAL_BUFFER_NUM - NVM_BUFFER_NUM) * (Size)BLCKSZ;
        t_thrd.storage_cxt.BufferBlocks = (char *)ShmemInitStruct("Buffer Blocks", buffer_size, &found_bufs);
    }
#endif

    if (g_instance.attr.attr_storage.nvm_attr.enable_nvm) {
        nvm_init();
    }

    if (BBOX_BLACKLIST_SHARE_BUFFER) {
        /* Segment Buffer is exclued from the black list, as it contains many critical information for debug */
        bbox_blacklist_add(SHARED_BUFFER, t_thrd.storage_cxt.BufferBlocks, NORMAL_SHARED_BUFFER_NUM * (Size)BLCKSZ);
    }

    /*
     * The array used to sort to-be-checkpointed buffer ids is located in
     * shared memory, to avoid having to allocate significant amounts of
     * memory at runtime. As that'd be in the middle of a checkpoint, or when
     * the checkpointer is restarted, memory allocation failures would be
     * painful.
     */
    g_instance.ckpt_cxt_ctl->CkptBufferIds =
        (CkptSortItem *)ShmemInitStruct("Checkpoint BufferIds",
                                        TOTAL_BUFFER_NUM * sizeof(CkptSortItem), &found_buf_ckpt);

    /* Init the snapshotBlockLock to block all the io in the process of snapshot of standy */
    if (g_instance.ckpt_cxt_ctl->snapshotBlockLock == NULL) {
        g_instance.ckpt_cxt_ctl->snapshotBlockLock = LWLockAssign(LWTRANCHE_IO_BLOCKED);
    }

    if (ENABLE_INCRE_CKPT && g_instance.ckpt_cxt_ctl->dirty_page_queue == NULL) {
        g_instance.ckpt_cxt_ctl->dirty_page_queue_size = TOTAL_BUFFER_NUM *
                                                         PAGE_QUEUE_SLOT_MULTI_NBUFFERS;
        MemoryContext oldcontext = MemoryContextSwitchTo(g_instance.increCheckPoint_context);

        Size queue_mem_size = g_instance.ckpt_cxt_ctl->dirty_page_queue_size * sizeof(DirtyPageQueueSlot);
        g_instance.ckpt_cxt_ctl->dirty_page_queue =
            (DirtyPageQueueSlot *)palloc_huge(CurrentMemoryContext, queue_mem_size);

        /* The memory of the memset sometimes exceeds 2 GB. so, memset_s cannot be used. */
        MemSet((char*)g_instance.ckpt_cxt_ctl->dirty_page_queue, 0, queue_mem_size);
        (void)MemoryContextSwitchTo(oldcontext);
    }

    if (g_instance.bgwriter_cxt.unlink_rel_hashtbl == NULL) {
        g_instance.bgwriter_cxt.unlink_rel_hashtbl = relfilenode_hashtbl_create("unlink_rel_hashtbl", true);
    }

    if (g_instance.bgwriter_cxt.unlink_rel_fork_hashtbl == NULL) {
        g_instance.bgwriter_cxt.unlink_rel_fork_hashtbl =
            relfilenode_fork_hashtbl_create("unlink_rel_one_fork_hashtbl", true);
    }

    if (found_descs || found_bufs || found_buf_ckpt || found_buf_extra) {
        /* both should be present or neither */
        Assert(found_descs && found_bufs && found_buf_ckpt && found_buf_extra);
        /* note: this path is only taken in EXEC_BACKEND case */
    } else {

        int i;

        /*
         * Initialize all the buffer headers.
         */
        for (i = 0; i < TOTAL_BUFFER_NUM; i++) {
            BufferDesc *buf = GetBufferDescriptor(i);
            CLEAR_BUFFERTAG(buf->tag);

            pg_atomic_init_u32(&buf->state, 0);
            buf->wait_backend_pid = 0;

            buf->extra = &extra[i];
            buf->buf_id = i;
            buf->io_in_progress_lock = LWLockAssign(LWTRANCHE_BUFFER_IO_IN_PROGRESS);
            buf->content_lock = LWLockAssign(LWTRANCHE_BUFFER_CONTENT);
            pg_atomic_init_u64(&buf->extra->rec_lsn, InvalidXLogRecPtr);
            buf->extra->aio_in_progress = false;
            buf->extra->dirty_queue_loc = PG_UINT64_MAX;
            buf->extra->encrypt = false;

            buf->prev = NULL;
            buf->next = NULL;
            buf->tenantOid = UINT32_MAX;
            // buf->extra->meta.sample_time = 0;
            // buf->extra->meta.access_count = 0;
            // buf->extra->meta.id = i;
            // buf->extra->meta.link = NULL;
        }
        g_instance.bgwriter_cxt.rel_hashtbl_lock = LWLockAssign(LWTRANCHE_UNLINK_REL_TBL);
        g_instance.bgwriter_cxt.rel_one_fork_hashtbl_lock = LWLockAssign(LWTRANCHE_UNLINK_REL_FORK_TBL);
    }


    /* re-assign locks for un-reinited buffers, may delete this */
    if (SS_PERFORMING_SWITCHOVER) {
        g_instance.bgwriter_cxt.rel_hashtbl_lock = LWLockAssign(LWTRANCHE_UNLINK_REL_TBL);
        g_instance.bgwriter_cxt.rel_one_fork_hashtbl_lock = LWLockAssign(LWTRANCHE_UNLINK_REL_FORK_TBL);
    }

    /* Init other shared buffer-management stuff */
    StrategyInitialize(!found_descs);

    /* Init Vector Buffer management stuff */
    DataCacheMgr::NewSingletonInstance();

    /* Initialize per-backend file flush context */
    WritebackContextInit(t_thrd.storage_cxt.BackendWritebackContext, &u_sess->attr.attr_common.backend_flush_after);

    if (ENABLE_DMS) {
        InitDmsBufCtrl();
    }
}

/*
 * BufferShmemSize
 *
 * compute the size of shared memory for the buffer pool including
 * data pages, buffer descriptors, hash tables, etc.
 */
Size BufferShmemSize(void)
{
    Size size = 0;

    /* size of buffer descriptors */
    size = add_size(size, mul_size(TOTAL_BUFFER_NUM + TWB_SIZE, sizeof(BufferDescPadded)));
    size = add_size(size, PG_CACHE_LINE_SIZE);
    size = add_size(size, mul_size(TOTAL_BUFFER_NUM + TWB_SIZE, sizeof(BufferDescExtra)));
    size = add_size(size, PG_CACHE_LINE_SIZE);

    /* size of data pages */
    size = add_size(size, mul_size((NORMAL_SHARED_BUFFER_NUM + SEGMENT_BUFFER_NUM + TWB_SIZE), BLCKSZ));
#ifdef __aarch64__
    size = add_size(size, PG_CACHE_LINE_SIZE);
#endif
    /* size of stuff controlled by freelist.c */
    size = add_size(size, StrategyShmemSize());

    /* size of checkpoint sort array in bufmgr.c */
    size = add_size(size, mul_size(TOTAL_BUFFER_NUM + TWB_SIZE, sizeof(CkptSortItem)));

    /* size of candidate buffers */
    size = add_size(size, mul_size(TOTAL_BUFFER_NUM * 5 , sizeof(Buffer)));

    /* size of candidate free map */
    size = add_size(size, mul_size(TOTAL_BUFFER_NUM + TWB_SIZE, sizeof(bool)));

    size = add_size(size, mul_size(EXTRA_MEM_FACTOR * TOTAL_BUFFER_NUM + TWB_SIZE, sizeof(buffer_node)));

    size = add_size(size, mul_size(2 * TOTAL_BUFFER_NUM, sizeof(BufferMeta)));

    size = add_size(size, mul_size(TOTAL_BUFFER_NUM * ( TENANT_NUM_PARAM + 1 ), sizeof(int)));

    size = add_size(size, hash_estimate_size(2 * TOTAL_BUFFER_NUM, sizeof(BufferMeta)));

    /* size of dms buf ctrl and buffer align */
    if (ENABLE_DMS) {
        size = add_size(size, mul_size(TOTAL_BUFFER_NUM, sizeof(dms_buf_ctrl_t))) + ALIGNOF_BUFFER + PG_CACHE_LINE_SIZE;
    }

    return size;
}

