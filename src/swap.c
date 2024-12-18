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
swapDataEntry *swapDataEntryCreate(int intention, int dbid, robj *key, long long expiretime, uint64_t version) {
    swapDataEntry *req = zmalloc(sizeof(swapDataEntry));
    req->intention = intention;
    req->dbid = dbid;
    req->key = key;
    req->val = NULL;
    req->expiretime = expiretime;
    req->version = version;
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

/* Retrieving an object from the swap database (RocksDB) and adding it back
 * into the memory. It involves decoding the object data, checking expiration
 * times, and updating object metadata such as frequency of use.*/
robj *swapIn(robj *key, int dbid) {
    char *val;
    size_t vallen;
    char *err = NULL;
    robj *o = NULL;
    swapDataRetrieval *r;
    mstime_t swap_latency;
    long long expiretime, lfu_freq;

    /* Start monitoring the latency for the swap-in operation. */
    latencyStartMonitor(swap_latency);

    /* Use RocksDB to get the value associated with the key from the specified cf_handles. */
    val = rocksdb_get_cf(server.swap->rocks->db,
                         server.swap->rocks->ropts,
                         server.swap->rocks->cf_handles[dbid],
                         key->ptr, sdslen(key->ptr),
                         &vallen,
                         &err);
    if (err != NULL) {
        serverLog(LL_WARNING, "Rocksdb get key error, key:%s, err: %s", (sds)key->ptr, err);
        return NULL;
    }

    /* Decode the retrieved data， if decoding fails, free the allocated memory and return NULL. */
    if ((r = swapDataDecodeObject(key, val, vallen)) == NULL) { 
        zlibc_free(val);
        return NULL;
    } else {
        o = r->val;
        expiretime = r->expiretime;
        lfu_freq = r->lfu_freq;
        swapDataRetrievalRelease(r);
    }

    /* If the current server is the master and the object has expired, delete the object from
     * RocksDB and free the object.*/
    if (iAmMaster() &&
        expiretime != -1 && expiretime < mstime()) {
        decrRefCount(o);
        rocksdb_delete_cf(server.swap->rocks->db,
                          server.swap->rocks->wopts,
                          server.swap->rocks->cf_handles[dbid],
                          key->ptr,
                          sdslen(key->ptr),
                          &err);
        if (err != NULL) {
            serverLog(LL_WARNING, "Rocksdb delete key error, key:%s, err: %s", (sds)key->ptr, err);
            return NULL;
        }
        server.stat_swap_in_expired_keys_skipped++;
    } else {
        /* Add the new object in the hash table */
        int added = dbAddRDBLoad(server.db+dbid,key->ptr,o);
        if (!added) {
            serverLog(LL_WARNING,"Rocksdb has duplicated key '%s' in DB %d",(sds)key->ptr,dbid);
            serverPanic("Duplicated key found in Rocksdb");
        }

        /* Set the expire time if needed */
        if (expiretime != -1) {
            setExpire(NULL, server.db+dbid, key, expiretime);
        }

        /* Set usage information (for eviction). */
        objectSetLRUOrLFU(o, lfu_freq, -1, LRU_CLOCK(), 1000);
    }

    zlibc_free(val);
    latencyEndMonitor(swap_latency);
    latencyAddSampleIfNeeded("swap-in", swap_latency);
    server.stat_swap_in_keys_total++;
    return o;
}

/* A general function to attempt data swapping. */
static void swapData(int intention, int dbid, robj *key) {
    /* Check if the server has swap functionality enabled,
     * return immediately if not */
    if (!server.swap_enabled) return;
    
    if (!server.maxmemory) {
        /* If the intention of the swap operation is to swap
         * out, retrieve the expiration time of the key */
        long long expire = -1;
        if (intention == SWAP_OUT)
            expire = getExpire(server.db+dbid, key);
        /* Get and increment the swap data version number,
         * used to track the order of swap operations */
        uint64_t version = server.swap->swap_data_version++;
        /* Create a swap data entry object, encapsulating
         * the relevant information for the swap operation */
        swapDataEntry *entry = swapDataEntryCreate(intention, dbid, key, expire, version);
        /* Add the created swap data entry to the tail of
         * the pending requests list for the current thread*/
        listAddNodeTail(server.swap->pending_reqs[threadId], entry);
    }
}

/* Executes a data swap-out operation. */
void swapOut(robj* key, int dbid) {
    /* If swap is not enabled, return immediately. */
    if (!server.swap_enabled) return;
    /* Call swapData function with SWAP_OUT operation
     * to move the swap-out process. */
    swapData(SWAP_OUT, dbid, key);
}

/* Executes a data swap delete operation. */
void swapDel(robj *key, int dbid) {
    /* If swap is not enabled, return immediately. */
    if (!server.swap_enabled) return;
    /* Call swapData function with SWAP_DEL operation
     * to delete the key from swap. */
    swapData(SWAP_DEL, dbid, key);
}
