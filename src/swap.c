/*
 * Copyright (c) 2024, Apusic
 * This software is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *        http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#include "server.h"
#include "swap.h"

#define KB 1024
#define MB (1024 * 1024)

#define SWAP_OK 0
#define SWAP_FAIL 1

#define ROCKSDB_DIR "rocksdb.data"

/* Initializes the RocksDB database. */
int rocksInit(void) {
    server.swap->rocks = zmalloc(sizeof(struct rocks));
    server.swap->rocks->snapshot = NULL;
    return rocksOpen(server.swap->rocks);
}

/* Opens or creates the RocksDB database. */
int rocksOpen(struct rocks *rocks) {
    char *errs = NULL;
    char **cf_names = zmalloc(sizeof(char *) * server.dbnum);
    rocksdb_options_t **cf_opts = zmalloc(sizeof(rocksdb_options_t *) * server.dbnum);
    rocks->cf_handles = zmalloc(sizeof(rocksdb_column_family_handle_t *) * server.dbnum);
    rocks->db_opts = rocksdb_options_create();
    rocksdb_options_set_create_if_missing(rocks->db_opts, 1);
    rocksdb_options_set_create_missing_column_families(rocks->db_opts, 1);
    rocksdb_options_optimize_for_point_lookup(rocks->db_opts, 1);

    /* Set compaction options. */
    rocksdb_options_set_min_write_buffer_number_to_merge(rocks->db_opts, 2);
    rocksdb_options_set_level0_file_num_compaction_trigger(rocks->db_opts, 2);
    rocksdb_options_set_max_bytes_for_level_base(rocks->db_opts, 256 * MB);
    rocksdb_options_compaction_readahead_size(rocks->db_opts, 2 * 1024 * 1024);

    /* Set log and manifest file size options. */
    rocksdb_options_set_max_background_jobs(rocks->db_opts, server.rocksdb_max_background_jobs);
    rocksdb_options_set_max_background_compactions(rocks->db_opts, server.rocksdb_max_background_compactions);
    rocksdb_options_set_max_background_flushes(rocks->db_opts, server.rocksdb_max_background_flushes);
    rocksdb_options_set_max_subcompactions(rocks->db_opts, server.rocksdb_max_subcompactions);
    rocksdb_options_set_max_open_files(rocks->db_opts, server.rocksdb_max_open_files);
    rocksdb_options_set_enable_pipelined_write(rocks->db_opts, server.rocksdb_enable_pipelined_write);

    /* Set log and manifest file size options. */
    rocksdb_options_set_max_manifest_file_size(rocks->db_opts, 64 * MB);
    rocksdb_options_set_max_log_file_size(rocks->db_opts, 256 * MB);
    rocksdb_options_set_keep_log_file_num(rocks->db_opts, 12);
    
    /* Set read options. */
    rocks->ropts = rocksdb_readoptions_create();
    rocksdb_readoptions_set_verify_checksums(rocks->ropts, 0);
    rocksdb_readoptions_set_fill_cache(rocks->ropts, 1);
    
    /* Set write options. */
    rocks->wopts = rocksdb_writeoptions_create();
    rocksdb_options_set_WAL_ttl_seconds(rocks->db_opts, server.rocksdb_WAL_ttl_seconds);
    rocksdb_options_set_WAL_size_limit_MB(rocks->db_opts, server.rocksdb_WAL_size_limit_MB);
    rocksdb_options_set_max_total_wal_size(rocks->db_opts, server.rocksdb_max_total_wal_size);
    
    /* Configure each database's column families. */
    for (int i = 0; i < server.dbnum; i++) {
        cf_names[i] = i == 0 ? sdsnew("default") : sdscatfmt(sdsempty(), "db%d", i);
        cf_opts[i] = rocksdb_options_create_copy(rocks->db_opts);
        rocksdb_options_set_compression(cf_opts[i], server.rocksdb_compression);
        rocksdb_options_set_level0_slowdown_writes_trigger(cf_opts[i], server.rocksdb_level0_slowdown_writes_trigger);
        rocksdb_options_set_disable_auto_compactions(cf_opts[i], server.rocksdb_disable_auto_compactions);
        rocksdb_options_set_enable_blob_files(cf_opts[i], server.rocksdb_enable_blob_files);
        rocksdb_options_set_enable_blob_gc(cf_opts[i], server.rocksdb_enable_blob_garbage_collection);
        rocksdb_options_set_min_blob_size(cf_opts[i], server.rocksdb_min_blob_size);
        rocksdb_options_set_blob_file_size(cf_opts[i], server.rocksdb_blob_file_size);
        rocksdb_options_set_blob_gc_age_cutoff(cf_opts[i], (double)server.rocksdb_blob_garbage_collection_age_cutoff_percentage / 100);
        rocksdb_options_set_blob_gc_force_threshold(cf_opts[i], (double)server.rocksdb_blob_garbage_collection_force_threshold_percentage / 100);
        rocksdb_options_set_max_write_buffer_number(cf_opts[i], server.rocksdb_max_write_buffer_number);
        rocksdb_options_set_target_file_size_base(cf_opts[i], server.rocksdb_target_file_size_base);
        rocksdb_options_set_write_buffer_size(cf_opts[i], server.rocksdb_write_buffer_size);
        rocksdb_options_set_max_bytes_for_level_base(cf_opts[i], server.rocksdb_max_bytes_for_level_base);
        rocksdb_options_set_max_bytes_for_level_multiplier(cf_opts[i], server.rocksdb_max_bytes_for_level_multiplier);
        rocksdb_options_set_level_compaction_dynamic_level_bytes(cf_opts[i], server.rocksdb_compaction_dynamic_level_bytes);

        rocksdb_block_based_table_options_t *block_opts = rocksdb_block_based_options_create();
        rocksdb_block_based_options_set_block_size(block_opts, server.rocksdb_block_size);
        rocksdb_block_based_options_set_cache_index_and_filter_blocks(block_opts, server.rocksdb_cache_index_and_filter_blocks);
        rocksdb_cache_t *data_cache = rocksdb_cache_create_lru(server.rocksdb_block_cache_size);
        rocksdb_block_based_options_set_block_cache(block_opts, data_cache);
        rocksdb_cache_destroy(data_cache);
        rocksdb_options_set_block_based_table_factory(cf_opts[i], block_opts);
        rocksdb_block_based_options_destroy(block_opts);
    }
    
    /* Open the database. */
    rocks->db = rocksdb_open_column_families(rocks->db_opts, ROCKSDB_DIR, server.dbnum, (const char *const *)cf_names,
                                             (const rocksdb_options_t *const *)cf_opts, rocks->cf_handles, &errs);
    if (errs != NULL) {
        serverLog(LL_WARNING, "Rocksdb open column families failed: %s", errs);
        zlibc_free(errs);
        return C_ERR;
    }
    
    /* Clean up resources. */
    for (int i = 0; i < server.dbnum; i++) {
        sdsfree(cf_names[i]);
        rocksdb_options_destroy(cf_opts[i]);
    }
    zfree(cf_names);
    zfree(cf_opts);
    serverLog(LL_NOTICE, "Rocksdb open success");
    return C_OK;
}

/* Closes the RocksDB database. */
void rocksClose(void) {
    struct rocks *rocks = server.swap->rocks;
    rocksdb_cancel_all_background_work(rocks->db, 1);
    if (rocks->snapshot)
        rocksdb_release_snapshot(rocks->db, rocks->snapshot);
    for (int i = 0; i < server.dbnum; i++) {
        rocksdb_column_family_handle_destroy(rocks->cf_handles[i]);
    }
    rocksdb_options_destroy(rocks->db_opts);
    rocksdb_writeoptions_destroy(rocks->wopts);
    rocksdb_readoptions_destroy(rocks->ropts);
    rocksdb_close(rocks->db);
}

/* Creates an adaptive LRU cache. */
adaptiveLRU *adaptiveLRUCreate(void) {
    struct adaptiveLRU *al;

    if ((al = zmalloc(sizeof(*al))) == NULL)
        return NULL;
    al->warm_aligned_list = listCreate();
    al->warm_unaligned_list = listCreate();
    al->hot_aligned_list = listCreate();
    al->hot_unaligned_list = listCreate();
    return al;
}

/* Releases the adaptive LRU cache. */
void adaptiveLRURelease(adaptiveLRU *al) {
    listRelease(al->warm_aligned_list);
    listRelease(al->warm_unaligned_list);
    listRelease(al->hot_aligned_list);
    listRelease(al->hot_unaligned_list);
    zfree(al);
}

/* Adds a node to the adaptive LRU cache. */
listNode *adaptiveLRUAdd(adaptiveLRU *al, void *val, int to) {
    list *targetList = NULL;

    switch (to) {
        case AL_WARM_ALIGNED_LOC: targetList = al->warm_aligned_list; break;
        case AL_WARM_UNALIGNED_LOC: targetList = al->warm_unaligned_list; break;
        case AL_HOT_ALIGNED_LOC: targetList = al->hot_aligned_list; break;
        case AL_HOT_UNALIGNED_LOC: targetList = al->hot_unaligned_list; break;
        default: serverPanic("Invalid adaptiveLRU type");
    }

    if (listAddNodeHead(targetList, val) == NULL)
        return NULL;
    else
        return listFirst(targetList);
}

/* Moves a node between lists in the adaptive LRU cache. */
listNode *moveNode(adaptiveLRU *al, listNode *node, int *from, int to) {
    void *val = node->value;
    adaptiveLRUDel(al, node, *from);

    *from = to;
    listNode *newNode = adaptiveLRUAdd(al, val, to);
    if (!newNode) {
        serverPanic("Failed to add node to list");
    }
    return newNode;
}

/* Converts a node in the adaptive LRU cache based on read/write operations. */
listNode *adaptiveLRUConvert(adaptiveLRU *al, listNode *node, int *from, int rw) {
    if (rw == AL_READ) {
        switch (*from) {
            case AL_WARM_ALIGNED_LOC:
                return moveNode(al, node, from, AL_HOT_ALIGNED_LOC);
            case AL_HOT_ALIGNED_LOC:
                return moveNode(al, node, from, AL_HOT_ALIGNED_LOC);
            case AL_WARM_UNALIGNED_LOC:
                return moveNode(al, node, from, AL_HOT_UNALIGNED_LOC);
            case AL_HOT_UNALIGNED_LOC:
                return moveNode(al, node, from, AL_HOT_UNALIGNED_LOC);
            default:
                serverPanic("Invalid from value");
        }
    } else if (rw == AL_WRITE) {
        switch (*from) {
            case AL_WARM_ALIGNED_LOC:
                return moveNode(al, node, from, AL_HOT_UNALIGNED_LOC);
            case AL_HOT_ALIGNED_LOC:
                return moveNode(al, node, from, AL_HOT_UNALIGNED_LOC);
            case AL_WARM_UNALIGNED_LOC:
                return moveNode(al, node, from, AL_HOT_UNALIGNED_LOC);
            case AL_HOT_UNALIGNED_LOC:
                return moveNode(al, node, from, AL_HOT_UNALIGNED_LOC);
            default:
                serverPanic("Invalid from value");
        }
    } else {
        serverPanic("Invalid rw value");
    }
}

/* Deletes a node from the adaptive LRU cache. */
void adaptiveLRUDel(adaptiveLRU *al, listNode *node, int from) {
    switch(from) {
        case AL_WARM_ALIGNED_LOC: listDelNode(al->warm_aligned_list, node); break;
        case AL_WARM_UNALIGNED_LOC: listDelNode(al->warm_unaligned_list, node); break;
        case AL_HOT_ALIGNED_LOC: listDelNode(al->hot_aligned_list, node); break;
        case AL_HOT_UNALIGNED_LOC: listDelNode(al->hot_unaligned_list, node); break;
        default: serverPanic("Invalid adaptiveLRU type");
    }
}

/* Creates and sets up the necessary resources for data retrieval in the swap process */
swapDataRetrieval *swapDataRetrievalCreate(int dbid, robj *val, long long expiretime, long long lfu_freq) {
    swapDataRetrieval *r = zmalloc(sizeof(swapDataRetrieval));
    r->dbid = dbid;
    r->val = val;
    r->expiretime = expiretime;
    r->lfu_freq = lfu_freq;
    return r;
}

/* Releases the swapDataRetrieval. */
void swapDataRetrievalRelease(swapDataRetrieval *r) {
    if (r == NULL) return;
    zfree(r);
}

/* Creates a new data entry for the swap process. */
swapDataEntry *swapDataEntryCreate(int intention, int dbid, robj *key, long long expiretime) {
    swapDataEntry *req = zmalloc(sizeof(swapDataEntry));
    req->intention = intention;
    req->dbid = dbid;
    req->key = key;
    req->val = NULL;
    req->expiretime = expiretime;
    incrRefCount(key);
    return req;
}

/* Releases the swapDataEntry. */
void swapDataEntryRelease(swapDataEntry *e) {
    if (e == NULL) return;
    decrRefCount(e->key);
    if (e->val)
        decrRefCount(e->val);
    zfree(e);
}

/* Encodes an object into a buffer for storage. */
static sds swapDataEncodeObject(swapDataEntry *req) {
    uint8_t b[1];
    rio payload;
    int rdb_compression = server.rdb_compression;
    server.rdb_compression = 0;

    serverAssert(server.maxmemory_policy & MAXMEMORY_FLAG_LFU);

    /* Init RIO */
    rioInitWithBuffer(&payload, sdsempty());

    /* Save the DB number */
    if (rdbSaveType(&payload, RDB_OPCODE_SELECTDB) == -1) goto werr;
    if (rdbSaveLen(&payload, req->dbid) == -1) goto werr;

    /* Save the expire time */
    if (req->expiretime != -1) {
        if (rdbSaveType(&payload,RDB_OPCODE_EXPIRETIME_MS) == -1) goto werr;
        if (rdbSaveMillisecondTime(&payload,req->expiretime) == -1) goto werr;
    }

    /* Save the LFU info. */
    b[0] = LFUDecrAndReturn(req->val);
    if (rdbSaveType(&payload, RDB_OPCODE_FREQ) == -1) goto werr;
    if (rioWrite(&payload, b,1) == 0) goto werr;

    /* Save type, value */
    if (rdbSaveObjectType(&payload, req->val) == -1) goto werr;
    if (rdbSaveObject(&payload, req->val, req->key) == -1) goto werr;

    /* Save EOF */
    if (rdbSaveType(&payload, RDB_OPCODE_EOF) == -1) goto werr;

    server.rdb_compression = rdb_compression;
    return payload.io.buffer.ptr;

werr:
    server.rdb_compression = rdb_compression;
    serverLog(LL_WARNING, "Failed to encode object");
    return NULL;
}

/* Decodes a serialized object from a buffer and adds it to the dict. */
static swapDataRetrieval *swapDataDecodeObject(robj *key, char *buf, size_t len) {
    rio payload;
    robj *val = NULL;
    swapDataRetrieval *r = NULL;
    int type, error, dbid = 0;
    long long lfu_freq = -1, expiretime = -1;
    rioInitWithCBuffer(&payload, buf, len);
    while (1) {
        /* Read type. */
        if ((type = rdbLoadType(&payload)) == -1)
            goto rerr;

        /* Handle special types. */
        if (type == RDB_OPCODE_SELECTDB) {
            if ((dbid = rdbLoadLen(&payload, NULL)) == -1) goto rerr;
            continue;
        } else if (type == RDB_OPCODE_EXPIRETIME_MS) {
            if ((expiretime = rdbLoadMillisecondTime(&payload, RDB_VERSION)) == -1)
                goto rerr;
            continue;
        } else if (type == RDB_OPCODE_FREQ) {
            uint8_t byte;
            if (rioRead(&payload, &byte,1) == 0)
                goto rerr;
            lfu_freq = byte;
            continue;
        } else if (type == RDB_OPCODE_EOF) {
            break;
        }

        /* Read value */
        val = rdbLoadObject(type, &payload, key->ptr, &error);
    
        /* Check if the key already expired. */
        if (val == NULL) {
            if (error == RDB_LOAD_ERR_EMPTY_KEY) {
                server.stat_swap_in_empty_keys_skipped++;
                serverLog(LL_WARNING, "Decode object skipping empty key: %s", (sds)key->ptr);
            } else {
                goto rerr;
            }
        } else {
            r = swapDataRetrievalCreate(dbid, val, expiretime, lfu_freq);
        }
    }
    return r;

rerr:
    serverLog(LL_WARNING, "Failed to decode object");
    return NULL;
}

/* Initializes the swap state */
void swapInit(void) {
    server.swap = zmalloc(sizeof(*server.swap));    
    server.swap->al = adaptiveLRUCreate();
    server.swap->swap_data_version = 0;

    if (rocksInit() == C_ERR) {
        serverLog(LL_WARNING, "Failed to initialize RocksDB");
        exit(1);
    }

    if (cuckooFilterInit(server.swap->cf, 
                         server.swap_cuckoofilter_size_for_level /
                         server.swap_cuckoofilter_bucket_size /
                         sizeof(CuckooFingerprint),
                         server.swap_cuckoofilter_bucket_size,
                         CF_DEFAULT_MAX_ITERATIONS,
                         CF_DEFAULT_EXPANSION) == -1) {
        serverLog(LL_WARNING, "Failed to initialize Cuckoo Filter");
        exit(1);
    }

    for (int iel = 0; iel < MAX_THREAD_VAR; ++iel) {
        if (iel < server.worker_threads_num || 
            (iel == MODULE_THREAD_ID && server.worker_threads_num > 1)) {
            server.swap->pending_reqs[iel] = listCreate();
        }
    }
}

/* Releases resources and performs cleanup operations related to the swap process. */
void swapRelease(void) {
    for (int iel = 0; iel < MAX_THREAD_VAR; ++iel) {
        if (iel < server.worker_threads_num || 
            (iel == MODULE_THREAD_ID && server.worker_threads_num > 1)) {
            listRelease(server.swap->pending_reqs[iel]);
        }
    }
    rocksClose();
}
