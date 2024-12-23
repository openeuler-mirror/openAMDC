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

static sds swapDataEncodeObject(swapDataEntry *entry);
static swapDataRetrieval *swapDataDecodeObject(robj *key, char *buf, size_t len);

/* Swap data entry batch filter type */
dictType swapBatchFilterType = {
    dictSdsHash,                /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCompare,          /* key compare */
    NULL,                       /* key destructor */
    NULL,                       /* val destructor */
    NULL,                       /* allow to expand */
};

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
swapDataEntry *swapDataEntryCreate(int intention, int dbid, robj *key, robj *val, long long expiretime, uint64_t version) {
    swapDataEntry *req = zmalloc(sizeof(swapDataEntry));
    req->intention = intention;
    req->dbid = dbid;
    req->key = key;
    req->val = val;
    req->expiretime = expiretime;
    req->version = version;
    incrRefCount(key);
    if (val)
        incrRefCount(val);
    return req;
}

/* Releases the swapDataEntry. */
void swapDataEntryRelease(swapDataEntry *entry) {
    if (entry == NULL) return;
    decrRefCount(entry->key);
    if (entry->val)
        decrRefCount(entry->val);
    zfree(entry);
}

/* Submitting a swap data entry to a batch. It ensures that the entry's intention is valid,
 * checks if the current batch is full, and either submits the batch or adds the entry to
 * the batch accordingly. */
static int swapDataEntrySubmit(swapDataEntry *entry, int idx) {
    /* Ensure that the entry's intention is either SWAP_OUT or SWAP_DEL. */
    serverAssert(entry->intention == SWAP_OUT || entry->intention == SWAP_DEL);

    /* If the batch is not full, add the entry to the current batch. */
    swapDataEntryBatchAdd(server.swap->batch[threadId], entry);

    /* Check if the current batch for the thread has reached its maximum size. */
    if (server.swap->batch[threadId]->count >= server.swap_data_entry_batch_size) {
        /* If the batch is full, submit the current batch and handle any errors. */
        if (swapDataEntryBatchSubmit(server.swap->batch[threadId], idx) == C_ERR)
            return C_ERR;

        /* Create a new batch for the thread after submitting the old one. */
        server.swap->batch[threadId] = swapDataEntryBatchCreate();
    }    

    return C_OK;
}

static int swapMoveKeyOutOfMemory(swapDataEntry *entry) {
    /* Ensure that the entry's intention is SAP_OUT. */
    serverAssert(entry->intention == SWAP_OUT);
    
    /* If there are any expired keys in the database, remove this key from the expires dictionary. */
    if (dictSize(server.db[entry->dbid].expires) > 0)
        dictDelete(server.db[entry->dbid].expires,entry->key->ptr);
    
    /* Attempt to unlink the key from the main dictionary. */
    dictEntry *de = dictUnlink(server.db[entry->dbid].dict, entry->key->ptr);
    if (de) {
        /* Free the unlinked dictionary entry to release resources. */
        dictFreeUnlinkedEntry(server.db[entry->dbid].dict,de);
        return 1;
    } else {
        return 0;
    }
}

/* Encodes an object into a buffer for storage. */
static sds swapDataEncodeObject(swapDataEntry *entry) {
    uint8_t b[1];
    rio payload;
    int rdb_compression = server.rdb_compression;
    server.rdb_compression = 0;

    serverAssert(server.swap->maxmemory_policy == SWAP_MAXMEMORY_FLAG_LFU);

    /* Init RIO */
    rioInitWithBuffer(&payload, sdsempty());

    /* Save the DB number */
    if (rdbSaveType(&payload, RDB_OPCODE_SELECTDB) == -1) goto werr;
    if (rdbSaveLen(&payload, entry->dbid) == -1) goto werr;

    /* Save the expire time */
    if (entry->expiretime != -1) {
        if (rdbSaveType(&payload,RDB_OPCODE_EXPIRETIME_MS) == -1) goto werr;
        if (rdbSaveMillisecondTime(&payload,entry->expiretime) == -1) goto werr;
    }

    /* Save the LFU info. */
    b[0] = LFUDecrAndReturn(entry->val);
    if (rdbSaveType(&payload, RDB_OPCODE_FREQ) == -1) goto werr;
    if (rioWrite(&payload, b,1) == 0) goto werr;

    /* Save type, value */
    if (rdbSaveObjectType(&payload, entry->val) == -1) goto werr;
    if (rdbSaveObject(&payload, entry->val, entry->key) == -1) goto werr;

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

/* Create and initialize a swap data entry batch object. */
swapDataEntryBatch *swapDataEntryBatchCreate(void) {
    struct swapDataEntryBatch *eb = zmalloc(sizeof(swapDataEntryBatch));
    eb->entries = eb->entry_buf;
    eb->capacity = SWAP_DATA_ENTRY_BATCH_BUFFER_SIZE;
    eb->count = 0;
    return eb;
}

/* Release the resources associated with a swap data entry batch object. */
void swapDataEntryBatchRelease(swapDataEntryBatch *eb) {
    if (eb == NULL) return;
    for (int i = 0; i < eb->count; i++) {
        swapDataEntry *entry = eb->entries[i];
        swapDataEntryRelease(entry);
    }
    if (eb->entries != eb->entry_buf) {
        zfree(eb->entries);
    }
    zfree(eb);
}

/* Add a swap data entry to the batch. */
void swapDataEntryBatchAdd(swapDataEntryBatch *eb, swapDataEntry *entry) {
    /* Assert that the entry's intention is either SWAP_OUT or SWAP_DEL. */
    serverAssert(entry->intention == SWAP_OUT || entry->intention == SWAP_DEL);
    /* Check if the batch is full. */
    if (eb->count == eb->capacity) {
        /* Double the capacity of the entries buffer. */
        eb->capacity = eb->capacity * 2;
        serverAssert(eb->capacity > eb->count);

        /* Check if the entries buffer is the initial internal buffer. */
        if (eb->entries == eb->entry_buf) {
            /* Allocate a new buffer with the increased capacity. */
            eb->entries = zmalloc(sizeof(swapDataEntry *) * eb->capacity);
            /* Copy the existing entries to the new buffer. */
            memcpy(eb->entries, eb->entry_buf, sizeof(swapDataEntry *) * eb->count);
        } else {
            /* Reallocate the existing buffer with the increased capacity. */
            eb->entries = zrealloc(eb->entry_buf, sizeof(swapDataEntry *) * eb->capacity);
        }
    }
    /* Add the entry to the batch. */
    eb->entries[eb->count++] = entry;
}

/* Submits a batch of swap data entries for processing. */
int swapDataEntryBatchSubmit(swapDataEntryBatch *eb, int idx) {
    /* Check if there are no swap flush threads running. */
    if (server.swap_flush_threads_num == 0) {
        /* Process the batch synchronously. */
        int status = swapDataEntryBatchProcess(eb);
        /* Release the batch after processing. */
        swapDataEntryBatchRelease(eb);
        /* Return the status of the batch processing. */
        return status;
    } else {
        UNUSED(idx);
        // TODO: Implement asynchronous swap processing
    }

    return C_OK;
}

/* Process a batch of swap data entries. */
int swapDataEntryBatchProcess(swapDataEntryBatch *eb) {
    /* Return immediately if there are no entries in the batch. */
    if (eb->count == 0) return C_OK;

    sds buf;
    int status = C_OK;
    char *err = NULL;
    dictEntry *de;
    dict *filter = dictCreate(&swapBatchFilterType, NULL);
    rocksdb_writebatch_t *wb = rocksdb_writebatch_create();

    /* Iterate over the entries in reverse order to add them to the filter dictionary. */
    for (int i = eb->count-1; i >=0; i--) {
        swapDataEntry *entry = eb->entries[i];
        if ((de = dictFind(filter, entry->key->ptr)) == NULL) {
            dictAdd(filter, entry->key->ptr, entry);
        }
    }

    /* Process each entry in the filter dictionary. */
    dictIterator *iter = dictGetIterator(filter);
    while((de = dictNext(iter)) != NULL) {
        swapDataEntry *entry = dictGetVal(de);
        /* Handle entries with intention to swap out. */
        if (entry->intention == SWAP_OUT) {
            /* Encode the object associated with the entry. */
            if ((buf = swapDataEncodeObject(entry)) == NULL) {
                serverLog(LL_WARNING, "Swap data encode object failed, key:%s", (sds)entry->key->ptr);
                status = C_ERR;
                goto cleanup;
            }
            /* Add the encoded object to the RocksDB write batch. */
            rocksdb_writebatch_put(wb, entry->key->ptr, sdslen(entry->key->ptr), buf, sdslen(buf));
            /* Free the encoded buffer. */
            sdsfree(buf);
        /* Handle entries with intention to delete. */
        } else if (entry->intention == SWAP_DEL) {
            /* Delete the key from RocksDB using the write batch. */
            rocksdb_writebatch_delete(wb, entry->key->ptr, sdslen(entry->key->ptr));
        } else {
            serverPanic("Invilid swap intention type: %d\n", entry->intention);
        }
    }

    /* Write the batch to RocksDB. */
    rocksdb_write(server.swap->rocks->db, server.swap->rocks->wopts, wb, &err);
    if (err != NULL) {
        serverLog(LL_WARNING, "Rocksdb write batch failed, err:%s", err);
        zlibc_free(err);
        status = C_ERR;
        goto cleanup;
    }

    /* Check if there are no swap flush threads running. */
    if (server.swap_flush_threads_num == 0) {
        /* Iterate over each entry in the dictionary. */
        iter = dictGetIterator(filter);
        while((de = dictNext(iter)) != NULL) {
            swapDataEntry *entry = dictGetVal(de);
            /* Check if the entry's intention is SWAP_OUT. */
            if (entry->intention == SWAP_OUT) {
                /* Move the key out of memory according to the swap-out policy. */
                serverAssert(swapMoveKeyOutOfMemory(entry));
                /* Insert the key into the cold filter. */
                if (cuckooFilterInsert(&server.swap->coldFilter,
                                       entry->key->ptr, 
                                       sdslen(entry->key->ptr)) != CUCKOO_FILTER_INSERTED) {
                    serverLog(LL_WARNING, "Cuckoo filter insert failed, key:%s", (sds)entry->key->ptr);
                    status = C_ERR;
                    goto cleanup;
                }
            }
        }
    }

cleanup:
    /* Release resources. */
    dictReleaseIterator(iter);
    dictRelease(filter);
    rocksdb_writebatch_destroy(wb);
    return status;
}

/* Initializes the swap state. */
void swapInit(void) {
    server.swap = zmalloc(sizeof(swapState));    
    server.swap->swap_data_version = 0;
    server.swap->maxmemory_policy = SWAP_MAXMEMORY_FLAG_LFU;

    /* Initialize RocksDB and log a warning if initialization fails. */
    if (rocksInit() == C_ERR) {
        serverLog(LL_WARNING, "Failed to initialize RocksDB");
        exit(1);
    }

    /* Initialize the Cuckoo Filter and log a warning if initialization fails. */
    if (cuckooFilterInit(&server.swap->coldFilter, 
                         server.swap_cuckoofilter_size_for_level /
                         server.swap_cuckoofilter_bucket_size /
                         sizeof(CuckooFingerprint),
                         server.swap_cuckoofilter_bucket_size,
                         CF_DEFAULT_MAX_ITERATIONS,
                         CF_DEFAULT_EXPANSION) == -1) {
        serverLog(LL_WARNING, "Failed to initialize Cuckoo Filter");
        exit(1);
    }

    /* Initialize swap data entry batches and pending entries lists for worker threads */
    for (int iel = 0; iel < MAX_THREAD_VAR; ++iel) {
        if (iel < server.worker_threads_num || 
            (iel == MODULE_THREAD_ID && server.worker_threads_num > 1)) {
            server.swap->batch[iel] = swapDataEntryBatchCreate();
            server.swap->pending_entries[iel] = listCreate();
        }
    }
}

/* Releases resources and performs cleanup operations related to the swap process. */
void swapRelease(void) {
    for (int iel = 0; iel < MAX_THREAD_VAR; ++iel) {
        if (iel < server.worker_threads_num || 
            (iel == MODULE_THREAD_ID && server.worker_threads_num > 1)) {
            if (server.swap->batch[iel]) {
                swapDataEntryBatchSubmit(server.swap->batch[iel], -1);
            }
            listRelease(server.swap->pending_entries[iel]);
        }
    }
    cuckooFilterFree(&server.swap->coldFilter);
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
static void swapData(int intention, robj *key, robj *val, int dbid) {
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
        swapDataEntry *entry = swapDataEntryCreate(intention, dbid, key, val, expire, version);
        /* Add the created swap data entry to the tail of
         * the pending requests list for the current thread*/
        listAddNodeTail(server.swap->pending_entries[threadId], entry);
    }
}

/* Executes a data swap-out operation. */
void swapOut(robj* key, robj *val, int dbid) {
    /* If swap is not enabled, return immediately. */
    if (!server.swap_enabled) return;
    /* Call swapData function with SWAP_OUT operation
     * to move the swap-out process. */
    swapData(SWAP_OUT, key, val, dbid);
}

/* Executes a data swap delete operation. */
void swapDel(robj *key, int dbid) {
    /* If swap is not enabled, return immediately. */
    if (!server.swap_enabled) return;
    /* Call swapData function with SWAP_DEL operation
     * to delete the key from swap. */
    swapData(SWAP_DEL, key, NULL, dbid);
}

/* Process pending swap data entries for a specific thread. */
void swapProcessPendingEntries(int iel) {
    /* If there are no pending entries, return early. */
    list *entries = server.swap->pending_entries[iel];
    if (listLength(entries) == 0) return;

    /* Iterate over the list of pending entries. */
    listIter li;
    listNode *ln;
    listRewind(entries, &li);
    while ((ln = listNext(&li))) {
        swapDataEntry *e = listNodeValue(ln);
        listDelNode(entries, ln);

        /* Submit the swap data entry to the swap system. */
        if (swapDataEntrySubmit(e, -1) == C_ERR) {
            /* If submission fails, free the swap data entry. */
            serverLog(LL_WARNING, "Failed to submit swap data entry");
        }
    }
}
