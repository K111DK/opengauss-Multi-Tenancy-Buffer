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
// static void hlist_buffer_init(buffer* buffer_cxt, uint32 capacity, const char* name, BufferType type){
//     Assert(buffer_cxt!=NULL);
//     Assert(name!=NULL);
//     Assert(type == LRU || type == CLOCK);
//     Assert(capacity > 0);
//     HASHCTL hctl;
//     int ret = memset_s(&hctl, sizeof(HASHCTL), 0, sizeof(HASHCTL));
//     securec_check(ret, "\0", "\0");
//     hctl.keysize = sizeof(BufferTag);//tag hash
//     hctl.entrysize = sizeof(buffer_node);//lru node
//     hctl.hash = tag_hash;
//     buffer_cxt->buffer_map = ShmemInitHash(name, 
//         capacity, capacity, 
//         &hctl, 
//         HASH_ELEM | HASH_FUNCTION | HASH_FIXED_SIZE);
    
//     buffer_cxt->dummy_head.next = &buffer_cxt->dummy_tail;
//     buffer_cxt->dummy_head.prev = NULL;
//     buffer_cxt->dummy_tail.prev = &buffer_cxt->dummy_head;
//     buffer_cxt->dummy_tail.next = NULL;
//     buffer_cxt->max_capacity = capacity;
//     buffer_cxt->curr_size = 0;
//     buffer_cxt->type = type;
// }
void InitMultiTenantBufferPool(void){

    HASHCTL hctl;
    int ret = memset_s(&hctl, sizeof(HASHCTL), 0, sizeof(HASHCTL));
    securec_check(ret, "\0", "\0");
    hctl.keysize = TENANT_NAME_LEN;
    hctl.entrysize = sizeof(tenant_name_mapping); // oid
    hctl.hash = string_hash;
    g_tenant_info.tenant_map = ShmemInitHash("tenant info hash", 
    64, 64, &hctl, HASH_ELEM | HASH_FUNCTION | HASH_FIXED_SIZE);

    bool found_descs = false;  
    /* Free pool init */
    g_tenant_info.buffer_pool = (Buffer *)
    ShmemInitStruct("MultiTenantBuffers", NORMAL_SHARED_BUFFER_NUM * sizeof(Buffer), &found_descs);
    if(!found_descs){
        /* Only happen once */
        MemSet((char*)g_tenant_info.buffer_pool, 0, NORMAL_SHARED_BUFFER_NUM * sizeof(Buffer));
        INIT_CANDIDATE_LIST(g_tenant_info.buffer_list, g_tenant_info.buffer_pool, NORMAL_SHARED_BUFFER_NUM ,0 ,0);
        g_tenant_info.buffer_list.buf_id_start = 0;
        for(int buf_id = 0; buf_id < NvmBufferStartID; buf_id++){
            buf_push(&g_tenant_info.buffer_list, buf_id);
        }
    }


    /* Evict history list should be fifo */
    HASHCTL hctl1;
    memset_s(&hctl1, sizeof(HASHCTL), 0, sizeof(HASHCTL));
    hctl1.keysize = sizeof(BufferTag);//tag hash
    hctl1.entrysize = sizeof(buffer_node);//lru node
    hctl1.hash = tag_hash;
    g_tenant_info.history_buffer.buffer_map = ShmemInitHash("Hist", 
    NORMAL_SHARED_BUFFER_NUM, NORMAL_SHARED_BUFFER_NUM, &hctl1, HASH_ELEM | HASH_FUNCTION | HASH_FIXED_SIZE);
    if(!found_descs){
        g_tenant_info.history_buffer.dummy_head.next = &g_tenant_info.history_buffer.dummy_tail;
        g_tenant_info.history_buffer.dummy_head.prev = NULL;
        g_tenant_info.history_buffer.dummy_tail.prev = &g_tenant_info.history_buffer.dummy_head;
        g_tenant_info.history_buffer.dummy_tail.next = NULL;
        g_tenant_info.history_buffer.max_capacity = NORMAL_SHARED_BUFFER_NUM;
        g_tenant_info.history_buffer.curr_size = 0;
    }
    
    /* Non tenant buffer */
    tenant_name_mapping* entry = (tenant_name_mapping*)hash_search(g_tenant_info.tenant_map, "Non Tenant Buffer", HASH_ENTER, &found_descs);
    if (!found_descs) {
        entry->tenant_oid = UINT32_MAX;
        pthread_mutex_init(&g_tenant_info.tenant_map_lock, NULL);
        /* Backup buffer */
        strcpy_s(g_tenant_info.non_tenant_buffer_cxt.tenant_name, TENANT_NAME_LEN, "Non Tenant Buffer");
        tenant_buffer_init(&g_tenant_info.non_tenant_buffer_cxt, CLOCK, CLOCK, MINIMAL_BUFFER_SIZE);
        g_tenant_info.non_tenant_buffer_cxt.tenant_oid = UINT32_MAX;
        g_tenant_info.total_promised += MINIMAL_BUFFER_SIZE;
    }
}


void InitBufferPool(void)
{
    bool found_bufs = false;
    bool found_descs = false;
    bool found_buf_ckpt = false;
    bool found_buf_extra = false;
    uint64 buffer_size;
    BufferDescExtra *extra = NULL;
    t_thrd.tenant_id = UINT32_MAX - 1;

    t_thrd.storage_cxt.BufferDescriptors = (BufferDescPadded *)CACHELINEALIGN(
        ShmemInitStruct("Buffer Descriptors",
                        TOTAL_BUFFER_NUM * sizeof(BufferDescPadded) + PG_CACHE_LINE_SIZE,
                        &found_descs));

    extra = (BufferDescExtra *)CACHELINEALIGN(
        ShmemInitStruct("Buffer Descriptors Extra",
                        TOTAL_BUFFER_NUM * sizeof(BufferDescExtra) + PG_CACHE_LINE_SIZE,
                        &found_buf_extra));

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

#if ENABLE_MULTI_TENANTCY
        /* We make sure this won't exec twice */
        ereport(WARNING, (errmsg("Total shared buffer num: %d", NORMAL_SHARED_BUFFER_NUM)));
        InitMultiTenantBufferPool();
#endif

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
    size = add_size(size, mul_size(TOTAL_BUFFER_NUM, sizeof(BufferDescPadded)));
    size = add_size(size, mul_size(64 * TOTAL_BUFFER_NUM, sizeof(buffer_node)));
    size = add_size(size, PG_CACHE_LINE_SIZE);
    size = add_size(size, mul_size(TOTAL_BUFFER_NUM, sizeof(BufferDescExtra)));
    size = add_size(size, PG_CACHE_LINE_SIZE);

    /* size of data pages */
    size = add_size(size, mul_size((NORMAL_SHARED_BUFFER_NUM + SEGMENT_BUFFER_NUM), BLCKSZ));
#ifdef __aarch64__
    size = add_size(size, PG_CACHE_LINE_SIZE);
#endif
    /* size of stuff controlled by freelist.c */
    size = add_size(size, StrategyShmemSize());

    /* size of checkpoint sort array in bufmgr.c */
    size = add_size(size, mul_size(TOTAL_BUFFER_NUM, sizeof(CkptSortItem)));

    /* size of candidate buffers */
    size = add_size(size, mul_size(TOTAL_BUFFER_NUM, sizeof(Buffer)));

    /* size of candidate free map */
    size = add_size(size, mul_size(TOTAL_BUFFER_NUM, sizeof(bool)));

    /* size of dms buf ctrl and buffer align */
    if (ENABLE_DMS) {
        size = add_size(size, mul_size(TOTAL_BUFFER_NUM, sizeof(dms_buf_ctrl_t))) + ALIGNOF_BUFFER + PG_CACHE_LINE_SIZE;
    }

    return size;
}

