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
#include <math.h>

#define KB 1024
#define MB (1024 * 1024)

#define ROCKSDB_DIR "rocksdb.data"

static sds swapDataEncodeObject(swapDataEntry *entry);
static swapDataRetrieval *swapDataDecodeObject(robj *key, char *buf, size_t len);
static void swapDataEntryBatchFinished(swapDataEntryBatch *eb, int async);

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
swapDataEntry *swapDataEntryCreate(int intention, int dbid, robj *key, robj *val, long long expiretime) {
    swapDataEntry *req = zmalloc(sizeof(swapDataEntry));
    req->intention = intention;
    req->dbid = dbid;
    req->key = key;
    req->val = val;
    req->expiretime = expiretime;
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

/* Moves a key out of memory by unlinking it from the main dictionary and freeing resources. */
static int swapMoveKeyOutOfMemory(swapDataEntry *entry, int async) {
    /* Ensure that the entry's intention is SAP_OUT. */
    serverAssert(entry->intention == SWAP_OUT);
    
    /* If the operation is asynchronous, check if the key exists in the database and 
     * compare versions to ensure the entry is up-to-date. */
    if (async) {
        robj *val;
        int cur_version, old_version;

        /* Look up the dictionary entry in the database using the provided key. */
        dictEntry *de = dictFind(server.db[entry->dbid].dict, entry->key->ptr);
        /* If the key does not exist in the database, return 0 (indicating no action needed). */
        if (de == NULL) {
            return 0;
        }

        /* Get the version of the value in the database and the entry's value. */
        val = dictGetVal(de);
        cur_version = getVersion(val);
        old_version = getVersion(entry->val);
        /* If the database value's version is newer than the entry's value, return 0 
         * (indicating no action needed as the database has a more recent version). */
        if (cur_version > old_version) {
            return 0;
        }
    }

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

    serverAssert(server.swap->hotmemory_policy == SWAP_HOTMEMORY_FLAG_LFU);

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
    eb->thread_id = threadId;
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
        /* Inserts the key into the cold filter and moves the key out of memory. */
        swapDataEntryBatchFinished(eb, 0);
        /* Release the batch after processing. */
        swapDataEntryBatchRelease(eb);
        /* Return the status of the batch processing. */
        return status;
    } else {
        /* Determine the thread index for handling swap flush operations. */
        if (idx == -1) {
            static int dist;
            if (dist < 0)
                dist = 0;
            /* Use the incremented value of dist to round-robin select a thread for load balancing. */
            idx = (dist++) % server.swap_flush_threads_num;
        } else {
            /* If idx is specified, ensure it falls within the valid range. */
            idx = idx % server.swap_flush_threads_num;
        }

        /* Select the appropriate swap thread and add the entry to its pending list. */
        swapThread *thread = server.swap->swap_threads + idx;
        /* Lock the thread to safely modify its pending entries list. */
        if (pthread_mutex_lock(&thread->lock) != 0) return C_ERR;
        /* Add the entry to the end of the thread's pending entries list. */
        listAddNodeTail(thread->pending_entries, eb);
        /* Signal the condition variable to wake up the thread if it's waiting. */
        if (pthread_cond_signal(&thread->cond) != 0) return C_ERR;
        /* Unlock the thread after modifying its pending entries list. */
        if (pthread_mutex_unlock(&thread->lock) != 0) return C_ERR;

        return C_OK;
    }
}

/* Process a batch of swap data entries. */
int swapDataEntryBatchProcess(swapDataEntryBatch *eb) {
    /* Return immediately if there are no entries in the batch. */
    if (eb->count == 0) return C_OK;

    sds buf;
    int status = C_OK;
    char *err = NULL;
    mstime_t swap_latency;
    dictEntry *de;
    dict *filter = dictCreate(&swapBatchFilterType, NULL);
    rocksdb_writebatch_t *wb = rocksdb_writebatch_create();

    /* Filter the data and keep the data of the latest version. */
    for (int i = eb->count-1; i >=0; i--) {
        swapDataEntry *entry = eb->entries[i];
        if ((de = dictFind(filter, entry->key->ptr)) == NULL) {
            dictAdd(filter, entry->key->ptr, entry);
        }
    }

    /* Start monitoring the latency for the swap-batch operation. */
    latencyStartMonitor(swap_latency);

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
            /* Increment the total count of swap out keys in the swap statistics. */
            server.stat_swap_out_keys_total++;
        /* Handle entries with intention to delete. */
        } else if (entry->intention == SWAP_DEL) {
            /* Delete the key from RocksDB using the write batch. */
            rocksdb_writebatch_delete(wb, entry->key->ptr, sdslen(entry->key->ptr));
            /* Increment the total count of deleted keys in the swap statistics. */
            server.stat_swap_del_keys_total++;
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

    latencyEndMonitor(swap_latency);
    latencyAddSampleIfNeeded("swap-batch", swap_latency);

cleanup:
    /* Release resources. */
    dictReleaseIterator(iter);
    dictRelease(filter);
    rocksdb_writebatch_destroy(wb);
    return status;
}

/* Inserts the key into the cold filter and moves the key out of memory according to the swap-out policy. */
static void swapDataEntryBatchFinished(swapDataEntryBatch *eb, int async) {
    /* Iterate through each entry in the batch. */
    for (int i = 0; i < eb->count; i++) { 
        swapDataEntry *entry = eb->entries[i];
        /* Check if the entry's intention is SWAP_OUT. */
        if (entry->intention == SWAP_OUT) {
            /* Insert the key into the cold filter. */
            if (cuckooFilterInsert(&server.swap->cold_filter,
                                   entry->key->ptr, 
                                   sdslen(entry->key->ptr)) != CUCKOO_FILTER_INSERTED) {
                serverLog(LL_WARNING, "Cuckoo filter insert failed, key:%s", (sds)entry->key->ptr);
            }
            /* Move the key out of memory according to the swap-out policy. */
            swapMoveKeyOutOfMemory(entry, async);
        }
    }
}

/* Handles the completion of a batch of swap data entries. */
static void swapDataEntryBatchComplete(void *var, aeAsyncCallback *callback) {
    UNUSED(callback);
    swapDataEntryBatch *eb = var;
    /* Inserts the key into the cold filter and moves the key out of memory. */
    swapDataEntryBatchFinished(eb, 1);
    /* Release the resources associated with the batch after processing. */
    swapDataEntryBatchRelease(eb);
}

/* Creates and initializes a pool of swap entries. */
swapPoolEntry *swapPoolEntryCreate(void) {
    int j;
    swapPoolEntry *ep = zmalloc(sizeof(*ep) * SWAP_POOL_SIZE);
    for (j = 0; j < SWAP_POOL_SIZE; j++) {
        ep[j].cost = 0;
        ep[j].key = NULL;
        ep[j].cached = sdsnewlen(NULL, SWAP_POOL_CACHED_SDS_SIZE);
        ep[j].dbid = 0;
    }
    return ep;
}

/* Releases a pool of swap entries. */
void swapPoolEntryRelease(swapPoolEntry *pool) {
    for (int i = 0; i < SWAP_POOL_SIZE; i++) {
        sdsfree(pool[i].cached);
    }
    zfree(pool);
}

/* Populate the swap pool with entries from the sample dictionary. */
void swapPoolPopulate(swapPoolEntry *pool, int dbid, dict *sampledict, dict *keydict) {
    /* Check if the server has swap functionality enabled,
     * return immediately if not */
    if (!server.swap_enabled) return;
    
    int j, k, count;
    dictEntry *samples[server.swap_hotmemory_samples];

    /* Retrieve a set of random keys from the sample dictionary. */
    count = dictGetSomeKeys(sampledict,samples,server.swap_hotmemory_samples);
    for (j = 0; j < count; j++) {
        unsigned long long idle, cost;
        sds key;
        robj *o;
        dictEntry *de;

        de = samples[j];
        key = dictGetKey(de);

        /* If the dictionary we are sampling from is not the main
         * dictionary (but the expires one) we need to lookup the key
         * again in the key dictionary to obtain the value object. */
        if (sampledict != keydict) de = dictFind(keydict, key);
        o = dictGetVal(de);

        /* Calculate the swap cost according to the policy. */
        if (server.swap->hotmemory_policy == SWAP_HOTMEMORY_FLAG_LFU) {
            /* When we use an LRU policy, we sort the keys by swap cost
             * so that we expire keys starting from greater swap cost. */
            idle = 255-LFUDecrAndReturn(o);
            /* Calculate the cost based on idle time and the computed size
             * of the object. */
            cost = idle*objectComputeSize(o, OBJ_COMPUTE_SIZE_DEF_SAMPLES);
        } else {
            serverPanic("Unknown swap policy in swapPoolPopulate()");
        }

        /* Insert the element inside the pool.
         * First, find the first empty bucket or the first populated
         * bucket that has an swap cost smaller than our swap cost. */
        k = 0;
        while (k < SWAP_POOL_SIZE &&
               pool[k].key &&
               pool[k].cost < cost) k++;
        if (k == 0 && pool[SWAP_POOL_SIZE-1].key != NULL) {
            /* Can't insert if the element is < the worst element we have
             * and there are no empty buckets. */
            continue;
        } else if (k < SWAP_POOL_SIZE && pool[k].key == NULL) {
            /* Inserting into empty position. No setup needed before insert. */
        } else {
            /* Inserting in the middle. Now k points to the first element
             * greater than the element to insert.  */
            if (pool[SWAP_POOL_SIZE-1].key == NULL) {
                /* Free space on the right? Insert at k shifting
                 * all the elements from k to end to the right. */

                /* Save SDS before overwriting. */
                sds cached = pool[SWAP_POOL_SIZE-1].cached;
                memmove(pool+k+1,pool+k,
                    sizeof(pool[0])*(SWAP_POOL_SIZE-k-1));
                pool[k].cached = cached;
            } else {
                /* No free space on right? Insert at k-1 */
                k--;
                /* Shift all elements on the left of k (included) to the
                 * left, so we discard the element with smaller swap cost. */
                sds cached = pool[0].cached; /* Save SDS before overwriting. */
                if (pool[0].key != pool[0].cached) sdsfree(pool[0].key);
                memmove(pool,pool+1,sizeof(pool[0])*k);
                pool[k].cached = cached;
            }
        }

        /* Try to reuse the cached SDS string allocated in the pool entry,
         * because allocating and deallocating this object is costly
         * (according to the profiler, not my fantasy. Remember:
         * premature optimization bla bla bla. */
        int klen = sdslen(key);
        if (klen > SWAP_POOL_CACHED_SDS_SIZE) {
            pool[k].key = sdsdup(key);
        } else {
            memcpy(pool[k].cached,key,klen+1);
            sdssetlen(pool[k].cached,klen);
            pool[k].key = pool[k].cached;
        }
        pool[k].cost = cost;
        pool[k].dbid = dbid;
    }
}

/* Main function for a swap thread that processes batches of swap data entries. */
void *swapThreadMain(void *arg) {
    char name[20];
    swapThread *thread = arg;

    /* Set the thread's name for identification purposes. */
    snprintf(name, sizeof(name), "swap_thd:#%d", thread->id);
    redis_set_thread_title(name);

    listIter li;
    listNode *ln;
    list *processing_queue;

    /* Infinite loop to continuously process pending entries. */
    while (1) {
        pthread_mutex_lock(&thread->lock);
        /* Wait until there are pending entries to process. */
        while (listLength(thread->pending_entries) == 0)
            pthread_cond_wait(&thread->cond, &thread->lock);

        /* Prepare a new processing queue and transfer all pending entries to it. */
        listRewind(thread->pending_entries, &li);
        processing_queue = listCreate();
        while ((ln = listNext(&li))) {
            swapDataEntryBatch *eb = listNodeValue(ln);
            listAddNodeHead(processing_queue, eb);
            listDelNode(thread->pending_entries, ln);
        }
        pthread_mutex_unlock(&thread->lock);

        /* Process each batch in the processing queue. */
        listRewind(processing_queue, &li);
        while ((ln = listNext(&li))) {
            swapDataEntryBatch *eb = listNodeValue(ln);
            /* Process the batch of swap data entries. */
            swapDataEntryBatchProcess(eb);
            /* Schedule the completion callback asynchronously. */
            aeAsyncFunction(server.el[eb->thread_id], swapDataEntryBatchComplete, eb, NULL, 1);
        }
        /* Release the processing queue after all batches have been processed. */
        listRelease(processing_queue);
    }

    return NULL;
}

/* Initializes the swap threads for handling swap operations. */
void swapThreadInit(void) {
    /* If no swap flush threads are configured, return immediately. */
    if (server.swap_flush_threads_num == 0) return;

    server.swap->swap_threads = zmalloc(sizeof(swapThread) * server.swap_flush_threads_num);
    for (int i = 0; i < server.swap_flush_threads_num; i++) {
        swapThread *thread = server.swap->swap_threads + i;
        thread->id = i;
        thread->pending_entries = listCreate();

        /* Initialize the mutex and condition variable for thread synchronization. */
        pthread_mutex_init(&thread->lock, NULL);
        pthread_cond_init(&thread->cond, NULL);

        /* Create the thread with a specified stack size and start it with the main function `swapThreadMain`. */
        pthread_attr_t tattr;
        pthread_attr_init(&tattr);
        pthread_attr_setstacksize(&tattr, 1 << 23); /* Set stack size to 8MB */

        /* Create the thread and handle any creation errors. */
        if (pthread_create(&thread->thread_id, &tattr, swapThreadMain, thread)) {
            serverLog(LL_WARNING,"Fatal: Can't initialize swapThreadMain.");
            exit(1);
        }
    }
}

/* Closes and cleans up the swap threads. */
void swapThreadClose(void) {
    /* Iterate through each configured swap thread. */
    for (int i = 0; i < server.swap_flush_threads_num; i++) {
        swapThread *thread = server.swap->swap_threads + i;
        /* Skip the current thread if it is calling this function. */
        if (thread->thread_id == pthread_self())
            continue;
        
        /* Cancel and join the thread if it is active. */
        if (thread->thread_id && pthread_cancel(thread->thread_id) == 0) {
            /* Release the list of pending entries for this thread. */
            listRelease(thread->pending_entries);
            /* Wait for the thread to terminate. */
            pthread_join(thread->thread_id, NULL);
            /* Log a message indicating the thread has been terminated. */
            serverLog(LL_WARNING, "Swap thread #%d terminated.", i);
        }
    }
}

/* Initializes the swap state. */
void swapInit(void) {
    server.swap = zmalloc(sizeof(swapState));    
    server.swap->swap_data_version = 0;
    server.swap->hotmemory_policy = SWAP_HOTMEMORY_FLAG_LFU;
    server.swap->pool = swapPoolEntryCreate();

    /* Initialize the swap thread, setting up necessary resources
     * and configurations for thread execution. */
    swapThreadInit();

    /* Initialize RocksDB and log a warning if initialization fails. */
    if (rocksInit() == C_ERR) {
        serverLog(LL_WARNING, "Failed to initialize RocksDB");
        exit(1);
    }

    /* Initialize the Cuckoo Filter and log a warning if initialization fails. */
    if (cuckooFilterInit(&server.swap->cold_filter, 
                         server.swap_cuckoofilter_size_for_level,
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
            swapFlushThread(iel);
            listRelease(server.swap->pending_entries[iel]);
        }
    }
    swapThreadClose();
    swapPoolEntryRelease(server.swap->pool);
    cuckooFilterFree(&server.swap->cold_filter);
    rocksClose();
}

/* Flushes the swap batch at iel worker thread. */
int swapFlushThread(int iel) {
    /* Check if the server has swap functionality enabled,
     * return immediately if not */
    if (!server.swap_enabled) return C_OK;
    
    if (server.swap->batch[iel]) {
        /* Submit the current batch for processing. */
        if (swapDataEntryBatchSubmit(server.swap->batch[iel], -1) == C_ERR) {
            return C_ERR;
        }
        /* Create a new batch for future entries. */
        server.swap->batch[iel] = swapDataEntryBatchCreate();
    }
    return C_OK;
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
                         key->ptr,
                         sdslen(key->ptr),
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

        /* Set usage information (for swap). */
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
    
    if (!server.swap_hotmemory) {
        /* If the intention of the swap operation is to swap
         * out, retrieve the expiration time of the key */
        long long expire = -1;
        if (intention == SWAP_OUT)
            expire = getExpire(server.db+dbid, key);
        /* Get and increment the swap data version number,
         * used to track the order of swap operations */
        setVersion(val, server.swap->swap_data_version++);
        /* Create a swap data entry object, encapsulating
         * the relevant information for the swap operation */
        swapDataEntry *entry = swapDataEntryCreate(intention, dbid, key, val, expire);
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

/* Get the memory status from the point of view of the hotmemory directive:
 * if the memory used is under the hotmemory setting then C_OK is returned.
 * Otherwise, if we are over the memory limit, the function returns
 * C_ERR.
 *
 * The function may return additional info via reference, only if the
 * pointers to the respective arguments is not NULL. Certain fields are
 * populated only when C_ERR is returned:
 *
 *  'total'     total amount of bytes used.
 *              (Populated both for C_ERR and C_OK)
 *
 *  'logical'   the amount of memory used minus the slaves/AOF buffers.
 *              (Populated when C_ERR is returned)
 *
 *  'tofree'    the amount of memory that should be released
 *              in order to return back into the memory limits.
 *              (Populated when C_ERR is returned)
 *
 *  'level'     this usually ranges from 0 to 1, and reports the amount of
 *              memory currently used. May be > 1 if we are over the memory
 *              limit.
 *              (Populated both for C_ERR and C_OK)
 */
int getSwapHotmemoryState(size_t *total, size_t *logical, size_t *tofree, float *level) {
    size_t mem_reported, mem_used, mem_tofree;

    /* Check if we are over the hotmemory usage limit. If we are not, no need
     * to subtract the slaves output buffers. We can just return ASAP. */
    mem_reported = zmalloc_used_memory();
    if (total) *total = mem_reported;

    /* We may return ASAP if there is no need to compute the level. */
    int return_ok_asap = !server.swap_hotmemory || mem_reported <= server.swap_hotmemory;
    if (return_ok_asap && !level) return C_OK;

    /* Remove the size of slaves output buffers and AOF buffer from the
     * count of used hotmemory. */
    mem_used = mem_reported;
    size_t overhead = freeMemoryGetNotCountedMemory();
    mem_used = (mem_used > overhead) ? mem_used-overhead : 0;

    /* Compute the ratio of hotmemory usage. */
    if (level) {
        if (!server.swap_hotmemory) {
            *level = 0;
        } else {
            *level = (float)mem_used / (float)server.swap_hotmemory;
        }
    }

    if (return_ok_asap) return C_OK;

    /* Check if we are still over the memory limit. */
    if (mem_used <= server.swap_hotmemory) return C_OK;

    /* Compute how much hotmemory we need to free. */
    mem_tofree = mem_used - server.swap_hotmemory;

    if (logical) *logical = mem_used;
    if (tofree) *tofree = mem_tofree;

    return C_ERR;
}

/* Return 1 if used memory is more than hotmemory after allocating more memory,
 * return 0 if not. OpenAMDC may reject user's requests or evict some keys if used
 * memory exceeds hotmemory, especially, when we allocate huge memory at once. */
int overSwapHotmemoryAfterAlloc(size_t moremem) {
    if (!server.swap_hotmemory) return  0; /* No limit. */

    /* Check quickly. */
    size_t mem_used = zmalloc_used_memory();
    if (mem_used + moremem <= server.swap_hotmemory) return 0;

    size_t overhead = freeMemoryGetNotCountedMemory();
    mem_used = (mem_used > overhead) ? mem_used - overhead : 0;
    return mem_used + moremem > server.swap_hotmemory;
}

/* The swapTimeProc is started when "hotmemory" has been breached and
 * could not immediately be resolved. This will spin the event loop with short
 * eviction cycles until the "hotmemory" condition has resolved or there are no
 * more evictable items.  */
static int isSwapProcRunning = 0;
static int swapTimeProc(
        struct aeEventLoop *eventLoop, long long id, void *clientData) {
    UNUSED(eventLoop);
    UNUSED(id);
    UNUSED(clientData);
    serverAssert(threadOwnLock());

    if (performSwapData() == SWAP_RUNNING) return 0;  /* keep swapping */

    /* For SWAP_OK - things are good, no need to keep swapping.
     * For SWAP_FAIL - there is nothing left to swap.  */
    isSwapProcRunning = 0;
    return AE_NOMORE;
}

/* Check if it's safe to perform swap data.
 *   Returns 1 if swap data can be performed.
 *   Returns 0 if swap data processing should be skipped. */
static int isSafeToPerformSwapData(void) {
    /* - There must be no script in timeout condition.
     * - Nor we are loading data right now.  */
    if (server.lua_timedout || server.loading) return 0;

    /* When clients are paused the dataset should be static not just from the
     * POV of clients not being able to write, but also from the POV of
     * swap of keys not being performed. */
    if (checkClientPauseTimeoutAndReturnIfPaused()) return 0;

    return 1;
}

/* Algorithm for converting tenacity (0-100) to a time limit.  */
static unsigned long swapTimeLimitUs(void) {
    serverAssert(server.swap_hotmemory_eviction_tenacity >= 0);
    serverAssert(server.swap_hotmemory_eviction_tenacity <= 100);

    if (server.swap_hotmemory_eviction_tenacity <= 10) {
        /* A linear progression from 0..500us */
        return 50uL * server.swap_hotmemory_eviction_tenacity;
    }

    if (server.swap_hotmemory_eviction_tenacity < 100) {
        /* A 15% geometric progression, resulting in a limit of ~2 min at tenacity==99  */
        return (unsigned long)(500.0 * pow(1.15, server.swap_hotmemory_eviction_tenacity - 10.0));
    }

    return ULONG_MAX;   /* No limit to swap time */
}

/* Check that memory usage is within the current "hotmemory" limit.  If over
 * "hotmemory", attempt to free memory by swapping data (if it's safe to do so).
 *
 * It's possible for openAMDC to suddenly be significantly over the "hotmemory"
 * setting.  This can happen if there is a large allocation (like a hash table
 * resize) or even if the "hotmemory" setting is manually adjusted.  Because of
 * this, it's important to swap for a managed period of time - otherwise openAMDC
 * would become unresponsive while swapping.
 *
 * The goal of this function is to improve the memory situation - not to
 * immediately resolve it.  In the case that some items have been evicted but
 * the "hotmemory" limit has not been achieved, an aeTimeProc will be started
 * which will continue to swap items until memory limits are achieved or
 * nothing more is evictable.
 *
 * This should be called before execution of commands.  If SWAP_FAIL is
 * returned, commands which will result in increased memory usage should be
 * rejected.
 *
 * Returns:
 *   SWAP_OK       - hotmemory is OK or it's not possible to perform swap data now
 *   SWAP_RUNNING  - hotmemory is over the limit, but swap data is still processing
 *   SWAP_FAIL     - hotmemory is over the limit, and there's nothing to swap
 * */
int performSwapData(void) {
    serverAssert(threadOwnLock());
    serverAssert(server.swap->hotmemory_policy == SWAP_HOTMEMORY_FLAG_LFU);
    if (!isSafeToPerformSwapData()) return SWAP_OK;

    long long keys_swapped = 0;
    size_t mem_reported, mem_tofree;
    long long mem_freed; /* May be negative */
    mstime_t latency, swap_latency;
    long long delta;
    int slaves = listLength(server.slaves);
    int result = SWAP_FAIL;

    if (getSwapHotmemoryState(&mem_reported,NULL,&mem_tofree,NULL) == C_OK)
        return SWAP_OK;

    unsigned long swap_time_limit_us = swapTimeLimitUs();

    mem_freed = 0;

    latencyStartMonitor(latency);

    monotime swapTimer;
    elapsedStart(&swapTimer);

    while (mem_freed < (long long)mem_tofree) {
        int k, i;
        sds bestkey = NULL;
        int bestdbid;
        redisDb *db;
        dict *dict;
        dictEntry *de;
        long long expire = -1;
        swapDataEntry *entry;
        swapPoolEntry *pool = server.swap->pool;

        while(bestkey == NULL) {
            unsigned long total_keys = 0, keys;

            /* We don't want to make local-db choices when expiring keys,
             * so to start populate the swap pool sampling keys from
             * every DB. */
            for (i = 0; i < server.dbnum; i++) {
                db = server.db+i;
                dict = db->dict;
                if ((keys = dictSize(dict)) != 0) {
                    swapPoolPopulate(pool, i, dict, db->dict);
                    total_keys += keys;
                }
            }
            if (!total_keys) break; /* No keys to swap. */

            /* Go backward from best to worst element to swap. */
            for (k = SWAP_POOL_SIZE-1; k >= 0; k--) {
                if (pool[k].key == NULL) continue;
                bestdbid = pool[k].dbid;
                de = dictFind(server.db[pool[k].dbid].dict,pool[k].key);

                /* Remove the entry from the pool. */
                if (pool[k].key != pool[k].cached)
                    sdsfree(pool[k].key);
                pool[k].key = NULL;
                pool[k].cost = 0;

                /* If the key exists, is our pick. Otherwise it is
                 * a ghost and we need to try the next element. */
                if (de) {
                    bestkey = dictGetKey(de);
                    break;
                } else {
                    /* Ghost... Iterate again. */
                }
            }
        }

        /* Finally swap the selected key. */
        if (bestkey) {
            db = server.db+bestdbid;
            robj *keyobj, *valobj;
            keyobj = createStringObject(bestkey,sdslen(bestkey));
            /* We compute the amount of memory freed by swap alone.
             * AOF and Output buffer memory will be freed eventually so
             * we only care about memory used by the key space. */
            delta = (long long) zmalloc_used_memory();
            latencyStartMonitor(swap_latency);
            expire = ((de = dictFind(db->expires,bestkey)) == NULL) ? -1 : dictGetSignedIntegerVal(de);
            valobj = ((de = dictFind(db->dict,bestkey)) == NULL) ? NULL : dictGetVal(de);
            setVersion(valobj, server.swap->swap_data_version++);
            entry = swapDataEntryCreate(SWAP_OUT, bestdbid, keyobj, valobj, expire);
            swapDataEntrySubmit(entry, -1);
            latencyEndMonitor(swap_latency);
            latencyAddSampleIfNeeded("hotmemory-trigger-swap-out", swap_latency);
            delta -= (long long) zmalloc_used_memory();
            mem_freed += delta;
            keys_swapped++;

            if (keys_swapped % server.swap_data_entry_batch_size == 0) {
                /* When the memory to free starts to be big enough, we may
                 * start spending so much time here that is impossible to
                 * deliver data to the replicas fast enough, so we force the
                 * transmission here inside the loop. */
                if (slaves) flushSlavesOutputBuffers();

                /* Normally our stop condition is the ability to release
                 * a fixed, pre-computed amount of memory. However when we
                 * are deleting objects in another thread, it's better to
                 * check, from time to time, if we already reached our target
                 * memory, since the "mem_freed" amount is computed only
                 * across the swapDataEntrySubmit() call, while the thread can
                 * release the memory all the time. */
                if (getSwapHotmemoryState(NULL,NULL,NULL,NULL) == C_OK) {
                    break;
                }

                /* After some time, exit the loop early - even if memory limit
                 * hasn't been reached.  If we suddenly need to free a lot of
                 * memory, don't want to spend too much time here. */
                if (elapsedUs(swapTimer) > swap_time_limit_us ||
                    server.swap_flush_threads_num > 0) {
                    /* We still need to free memory - start swap timer proc. */
                    if (!isSwapProcRunning) {
                        isSwapProcRunning = 1;
                        aeCreateTimeEvent(server.el[threadId], 0,
                                swapTimeProc, NULL, NULL);
                    }
                    break;
                }
            }
        } else {
            goto cant_free; /* nothing to swap... */
        }
    }
    /* at this point, the memory is OK, or we have reached the time limit */
    result = (isSwapProcRunning) ? SWAP_RUNNING : SWAP_OK;

cant_free:
    if (result == SWAP_FAIL) {
        /* At this point, we have run out of swappable items. It's possible
         * that some items are being freed in the lazyfree thread. Perform a
         * short wait here if such jobs exist, but don't wait long.  */
        mstime_t swap_latency;
        latencyStartMonitor(swap_latency);
        while (swapFlushThread(threadId) == C_OK &&
              elapsedUs(swapTimer) < swap_time_limit_us) {
            if (getSwapHotmemoryState(NULL,NULL,NULL,NULL) == C_OK) {
                result = SWAP_OK;
                break;
            }
            usleep(swap_time_limit_us < 1000 ? swap_time_limit_us : 1000);
        }
        latencyEndMonitor(swap_latency);
        latencyAddSampleIfNeeded("swap-flush",swap_latency);
    }

    latencyEndMonitor(latency);
    latencyAddSampleIfNeeded("swap-cycle",latency);
    server.stat_swap_out_keys_total += keys_swapped;
    return result;
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
        swapDataEntrySubmit(e, -1);
    }
}
