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

static sds swapDataEncodeObject(swapDataEntry *entry);
static void swapDataEntryBatchFinished(swapDataEntryBatch *eb, int async);

/* Initializes the RocksDB database. */
int rocksInit(void) {
    server.swap->rocks = zmalloc(sizeof(struct rocks));
    server.swap->rocks->snapshot = NULL;
    return rocksOpen(server.swap->rocks);
}

/* Opens or creates the RocksDB database. */
int rocksOpen(struct rocks *rocks) {
    char *err = NULL;
    int num_column_families = server.dbnum + 1;
    char **cf_names = zmalloc(sizeof(char *) * num_column_families);
    rocksdb_options_t **cf_opts = zmalloc(sizeof(rocksdb_options_t *) * num_column_families);
    rocks->cf_handles = zmalloc(sizeof(rocksdb_column_family_handle_t *) * num_column_families);
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

    /* Configure each column families. */
    for (int i = 0; i < num_column_families; i++) {
        /* Default column is used to store meta information. */
        cf_names[i] = i == 0 ? sdsnew("default") : sdscatfmt(sdsempty(), "db%i", i-1);
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
    rocks->db = rocksdb_open_column_families(rocks->db_opts, server.rocksdb_dir, num_column_families, (const char *const *)cf_names,
                                             (const rocksdb_options_t *const *)cf_opts, rocks->cf_handles, &err);
    if (err != NULL) {
        serverLog(LL_WARNING, "Rocksdb open column families failed: %s", err);
        zlibc_free(err);
        return C_ERR;
    }
    
    /* Clean up resources. */
    for (int i = 0; i < num_column_families; i++) {
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
    int num_column_families = server.dbnum + 1;
    struct rocks *rocks = server.swap->rocks;
    rocksdb_cancel_all_background_work(rocks->db, 1);
    if (rocks->snapshot)
        rocksdb_release_snapshot(rocks->db, rocks->snapshot);
    for (int i = 0; i < num_column_families; i++) {
        rocksdb_column_family_handle_destroy(rocks->cf_handles[i]);
    }
    rocksdb_options_destroy(rocks->db_opts);
    rocksdb_writeoptions_destroy(rocks->wopts);
    rocksdb_readoptions_destroy(rocks->ropts);
    rocksdb_close(rocks->db);
}

/* Creates and sets up the necessary resources for data retrieval in the swap process */
swapDataRetrieval *swapDataRetrievalCreate(int dbid, robj *val, long long expiretime, long long lfu_freq, uint64_t version) {
    swapDataRetrieval *r = zmalloc(sizeof(swapDataRetrieval));
    r->dbid = dbid;
    r->val = val;
    r->expiretime = expiretime;
    r->lfu_freq = lfu_freq;
    r->version = version;
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
int swapDataEntrySubmit(swapDataEntry *entry, int idx, int force) {
    /* Ensure that the entry's intention is either SWAP_OUT or SWAP_DEL. */
    serverAssert(entry->intention == SWAP_OUT || entry->intention == SWAP_DEL);

    /* If the batch is not full, add the entry to the current batch. */
    swapDataEntryBatchAdd(server.swap->batch[threadId], entry);

    /* Check if the current batch for the thread has reached its maximum size. */
    if (server.swap->batch[threadId]->count >= server.swap_data_entry_batch_size || force) {
        /* If the batch is full, submit the current batch and handle any errors. */
        if (swapDataEntryBatchSubmit(server.swap->batch[threadId], idx, force) == C_ERR)
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

    /* Increment the cold data size in the database. */
    server.db[entry->dbid].cold_data_size++;

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

    /* Save obj version */
    if (rdbSaveType(&payload, RDB_OPCODE_VERSION) == -1) goto werr;
    if (rdbSaveLen(&payload, entry->version) == -1) goto werr;

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
swapDataRetrieval *swapDataDecodeObject(robj *key, char *buf, size_t len) {
    rio payload;
    robj *val = NULL;
    swapDataRetrieval *r = NULL;
    int type, error;
    uint64_t version = 0, dbid = 0;
    long long lfu_freq = -1, expiretime = -1;
    rioInitWithCBuffer(&payload, buf, len);
    while (1) {
        /* Read type. */
        if ((type = rdbLoadType(&payload)) == -1)
            goto rerr;

        /* Handle special types. */
        if (type == RDB_OPCODE_SELECTDB) {
            if ((dbid = rdbLoadLen(&payload, NULL)) == RDB_LENERR) goto rerr;
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
        } else if (type == RDB_OPCODE_VERSION) {
            if ((version = rdbLoadLen(&payload, NULL)) == RDB_LENERR) goto rerr;
            continue;
        } else if (type == RDB_OPCODE_EOF) {
            break;
        }

        /* Read value */
        val = rdbLoadObject(type, &payload, key->ptr, &error);
    
        /* Check if the key already expired. */
        if (val == NULL) {
            if (error == RDB_LOAD_ERR_EMPTY_KEY) {
                server.db[dbid].stat_swap_in_empty_keys_skipped++;
                serverLog(LL_WARNING, "Decode object skipping empty key: %s", (sds)key->ptr);
            } else {
                goto rerr;
            }
        } else {
            r = swapDataRetrievalCreate((int)dbid, val, expiretime, lfu_freq, version);
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
int swapDataEntryBatchSubmit(swapDataEntryBatch *eb, int idx, int force) {
    /* Check if there are no swap flush threads running. */
    if (server.swap_flush_threads_num == 0 || force) {
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

    sds buf = NULL;
    char *err = NULL;
    mstime_t swap_latency;
    cuckooFilter filter;
    rocksdb_writebatch_t *batch;

    /* Initialize the Cuckoo Filter if there are multiple entries in the batch. */
    if (eb->count > 1 &&
        cuckooFilterInit(&filter,
                         eb->count * sizeof(CuckooFingerprint),
                         server.swap_cuckoofilter_bucket_size,
                         CF_DEFAULT_MAX_ITERATIONS,
                         CF_DEFAULT_EXPANSION, 1) == -1) {
        serverLog(LL_WARNING, "Failed to initialize Cuckoo Filter");
        return C_ERR;
    }

    /* Create a rocksdb write batch. */
    batch = rocksdb_writebatch_create();
    /* Start monitoring the latency for the swap-batch operation. */
    latencyStartMonitor(swap_latency);

    /* Filter the data and keep the data of the latest version. */
    for (int i = eb->count-1; i >= 0; i--) {
        swapDataEntry *entry = eb->entries[i];
        /* Check if the entry's key is already in the filter. */
        if (eb->count > 1 &&
            cuckooFilterContains(&filter, entry->key->ptr, sdslen(entry->key->ptr))) {
            continue;
        }
        /* Handle entries with intention to swap out. */
        if (entry->intention == SWAP_OUT) {
            /* Encode the object associated with the entry. */
            if ((buf = swapDataEncodeObject(entry)) == NULL) {
                serverLog(LL_WARNING, "Swap data encode object failed, key:%s", (sds)entry->key->ptr);
                goto cleanup;
            }
            /* Write the encoded object to the rocksdb batch. */
            rocksdb_writebatch_put_cf(batch,
                                      server.swap->rocks->cf_handles[DB_CF(entry->dbid)],
                                      entry->key->ptr,
                                      sdslen(entry->key->ptr),
                                      buf,
                                      sdslen(buf));
            /* Free the encoded buffer. */
            sdsfree(buf);
            /* Increment the total count of swap out keys in the swap statistics. */
            server.db[entry->dbid].stat_swap_out_keys_total++;
        /* Handle entries with intention to delete. */
        } else if (entry->intention == SWAP_DEL) {
            /* Delete the key from rocksdb batch. */
            rocksdb_writebatch_delete_cf(batch,
                                         server.swap->rocks->cf_handles[DB_CF(entry->dbid)],
                                         entry->key->ptr,
                                         sdslen(entry->key->ptr));
            /* Increment the total count of deleted keys in the swap statistics. */
            server.db[entry->dbid].stat_swap_del_keys_total++;
        } else {
            serverPanic("Invilid swap intention type: %d\n", entry->intention);
        }
        /* Insert the key into the filter if it's a batch operation. */
        if (eb->count > 1) {
            cuckooFilterInsert(&filter, entry->key->ptr, sdslen(entry->key->ptr));
        }
    }

    /* Write the batch to rocksdb. */
    rocksdb_write(server.swap->rocks->db,
                  server.swap->rocks->wopts,
                  batch,
                  &err);
    if (err != NULL) {
        serverLog(LL_WARNING, "Rocksdb write batch failed, err:%s", err);
        zlibc_free(err);
        goto cleanup;
    }

    latencyEndMonitor(swap_latency);
    latencyAddSampleIfNeeded("swap-batch", swap_latency);
    if (eb->count > 1) cuckooFilterFree(&filter);
    rocksdb_writebatch_destroy(batch);
    return C_OK;

cleanup:
    if (buf != NULL) sdsfree(buf);
    if (eb->count > 1) cuckooFilterFree(&filter);
    rocksdb_writebatch_destroy(batch);
    return C_ERR;
}

/* Inserts the key into the cold filter and moves the key out of memory according to the swap-out policy. */
static void swapDataEntryBatchFinished(swapDataEntryBatch *eb, int async) {
    /* Iterate through each entry in the batch. */
    for (int i = 0; i < eb->count; i++) { 
        swapDataEntry *entry = eb->entries[i];
        /* Check if the entry's intention is SWAP_OUT. */
        if (entry->intention == SWAP_OUT) {
            /* Insert the key into the cold filter. */
            if (cuckooFilterInsert(&server.swap->cold_filter[entry->dbid],
                                   entry->key->ptr, 
                                   sdslen(entry->key->ptr)) != CUCKOO_FILTER_INSERTED) {
                serverLog(LL_WARNING, "Cuckoo filter insert failed, key:%s", (sds)entry->key->ptr);
            }
            /* Move the key out of memory according to the swap-out policy. */
            swapMoveKeyOutOfMemory(entry, async);
        } else if (entry->intention == SWAP_DEL) {
            /* Delete the key from the cold filter. */
            cuckooFilterDelete(&server.swap->cold_filter[entry->dbid],
                               entry->key->ptr, 
                               sdslen(entry->key->ptr));
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
         * according to the profiler, not my fantasy. Remember:
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
    uint8_t hashseed[16];
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
    server.swap->cold_filter = zmalloc(sizeof(cuckooFilter)*server.dbnum);
    for (int i = 0; i < server.dbnum; ++i) {
        if (cuckooFilterInit(&server.swap->cold_filter[i],
                             server.swap_cuckoofilter_size_for_level,
                             server.swap_cuckoofilter_bucket_size,
                             CF_DEFAULT_MAX_ITERATIONS,
                             CF_DEFAULT_EXPANSION, 1) == -1) {
            serverLog(LL_WARNING, "Failed to initialize Cuckoo Filter");
            exit(1);
        }
    }

    /* Initialize swap data entry batches and pending entries lists for worker threads */
    for (int iel = 0; iel < MAX_THREAD_VAR; ++iel) {
        if (iel < server.worker_threads_num || 
            (iel == MODULE_THREAD_ID && server.worker_threads_num > 1)) {
            server.swap->batch[iel] = swapDataEntryBatchCreate();
            server.swap->pending_entries[iel] = listCreate();
        }
    }
    
    /* Initialize hash seed. */
    getRandomBytes(hashseed,sizeof(hashseed));
    cuckooFilterSetHashFunctionSeed(hashseed);
}

/* Releases resources and performs cleanup operations related to the swap process. */
void swapRelease(void) {
    /* Flushes the swap batch first. */
    for (int iel = 0; iel < MAX_THREAD_VAR; ++iel) {
        if (iel < server.worker_threads_num || 
            (iel == MODULE_THREAD_ID && server.worker_threads_num > 1)) {
            swapFlushThread(iel, 1);
            listRelease(server.swap->pending_entries[iel]);
        }
    }
    /* Swap hot data to RocksDB. */
    swapHotmemorySave();
    /* Close the swap threads. */
    swapThreadClose();
    /* Release the swap pool. */
    swapPoolEntryRelease(server.swap->pool);
    /* Release the Cuckoo Filter. */
    for (int i = 0; i < server.dbnum; ++i)
        cuckooFilterFree(&server.swap->cold_filter[i]);
    zfree(server.swap->cold_filter);
    /* Close the RocksDB */
    rocksClose();
}

/* Flushes the swap batch at iel worker thread. */
int swapFlushThread(int iel, int force) {
    /* Check if the server has swap functionality enabled,
     * return immediately if not */
    if (!server.swap_enabled) return C_OK;
    
    if (server.swap->batch[iel]) {
        /* Submit the current batch for processing. */
        if (swapDataEntryBatchSubmit(server.swap->batch[iel], -1, force) == C_ERR) {
            return C_ERR;
        }
        /* Create a new batch for future entries. */
        server.swap->batch[iel] = swapDataEntryBatchCreate();
    }
    return C_OK;
}

/* Flushes all swap batches at iel worker thread. */
int swapFlushAllThread(int force) {
    /* Check if the server has swap functionality enabled,
     * return immediately if not */
    if (!server.swap_enabled) return C_OK;

    for (int iel = 0; iel < MAX_THREAD_VAR; ++iel) {
        if (iel < server.worker_threads_num || 
            (iel == MODULE_THREAD_ID && server.worker_threads_num > 1)) {
            if (swapFlushThread(iel, force) == C_ERR) {
                return C_ERR;
            }
        }
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
    uint64_t version;
    long long expiretime, lfu_freq;

    /* Start monitoring the latency for the swap-in operation. */
    latencyStartMonitor(swap_latency);

    /* Use RocksDB to get the value associated with the key from the specified cf_handles. */
    val = rocksdb_get_cf(server.swap->rocks->db,
                         server.swap->rocks->ropts,
                         server.swap->rocks->cf_handles[DB_CF(dbid)],
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
        version = r->version;
        swapDataRetrievalRelease(r);
    }

    /* If the current server is the master and the object has expired, delete the object from
     * RocksDB and free the object.*/
    if (iAmMaster() &&
        expiretime != -1 && expiretime < mstime()) {
        decrRefCount(o);
        /* Delete the key from RocksDB. */
        rocksdb_delete_cf(server.swap->rocks->db,
                          server.swap->rocks->wopts,
                          server.swap->rocks->cf_handles[DB_CF(dbid)],
                          key->ptr,
                          sdslen(key->ptr),
                          &err);
        if (err != NULL) {
            serverLog(LL_WARNING, "Rocksdb delete key error, key:%s, err: %s", (sds)key->ptr, err);
            return NULL;
        }
        /* Remove the key from the expire dict. */
        dictDelete(server.db[dbid].expires, key->ptr);
        /* Remove the key from the cold filter. */
        cuckooFilterDelete(&server.swap->cold_filter[dbid], key->ptr, sdslen(key->ptr));
        server.db[dbid].cold_data_size--;
        server.db[dbid].stat_swap_in_expired_keys_skipped++;
        zlibc_free(val);
        return NULL;
    } else {
        /* Add the new object in the hash table. */
        sds copy = sdsdup(key->ptr);
        int added = dbAddRDBLoad(server.db+dbid,copy,o);
        if (!added) {
            serverLog(LL_WARNING,"Rocksdb has duplicated key '%s' in DB %d",(sds)key->ptr,dbid);
            serverPanic("Duplicated key found in Rocksdb");
        }

        /* Set usage information (for swap). */
        objectSetLRUOrLFU(o, lfu_freq, -1, LRU_CLOCK(), 1000);

        /* Set the version of the object. */
        setVersion(o, version);

        /* End monitoring the latency and add the sample to the latency monitor */
        latencyEndMonitor(swap_latency);
        latencyAddSampleIfNeeded("swap-in", swap_latency);
        server.db[dbid].stat_swap_in_keys_total++;
        zlibc_free(val);
        return o;
    }
}

/* A general function to attempt data swapping. */
static void swapData(int intention, robj *key, robj *val, int dbid) {
    /* Check if the server has swap functionality enabled,
     * return immediately if not */
    if (!server.swap_enabled) return;

    if (!server.swap_hotmemory) {
        /* If the intention of the swap operation is to swap out. */
        uint64_t version = 0;
        long long expire = -1;
        if (intention == SWAP_OUT) {
            /* Retrieve the expiration time of the key. */
            expire = getExpire(server.db+dbid, key);
            /* Retrieve the version of the object. */
            version = getVersion(val);
        }
        /* Create a swap data entry object, encapsulating
         * the relevant information for the swap operation */
        swapDataEntry *entry = swapDataEntryCreate(intention, dbid, key, val, expire, version);
        /* Add the created swap data entry to the tail of
         * the pending requests list for the current thread. */
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

/* Load hot data from RocksDB. */
int swapHotMemoryLoad(void) {
    if (!server.swap_enabled) return C_ERR;

    char *err = NULL;
    int dbid, dist = 0, db = 0;
    rocksdb_iterator_t *iter;
    rocksdb_iterator_t **iterators = NULL;
    long long dbnum, cold_data_size, swap_data_version, keys_loaded = 0;
    long long delta, mem_toload, mem_loaded = 0;
    size_t mem_reported, mem_used;

    /* Allocate memory for iterators */
    iterators = zcalloc(sizeof(rocksdb_iterator_t *) * (server.dbnum + 1));
    rocksdb_create_iterators(server.swap->rocks->db,
                             server.swap->rocks->ropts,
                             server.swap->rocks->cf_handles,
                             iterators,
                             server.dbnum + 1,
                             &err);
    if (err != NULL) {
        serverLog(LL_WARNING, "Rocksdb create iterators error, err: %s", err);
        goto cleanup;
    }

    /* Process the current meta info. */
    iter = iterators[META_CF];
    for (rocksdb_iter_seek_to_first(iter);
         rocksdb_iter_valid(iter);
         rocksdb_iter_next(iter)) {
        size_t klen, vlen;
        char *key_buf = (char *)rocksdb_iter_key(iter, &klen);
        char *val_buf = (char *)rocksdb_iter_value(iter, &vlen);

        if (val_buf == NULL) goto cleanup;
        
        if (!strncmp(key_buf, "dbnum", 5)) {
            /* Load the number of databases (dbnum) from RocksDB. */
            /* Convert the value and check if the dbnum exceeds the configured maximum. */
            if (string2ll(val_buf, vlen, &dbnum) == 0) goto cleanup;
            
            if ((int)dbnum < server.dbnum) {
                serverLog(LL_WARNING,
                    "FATAL: Rocksdb was created with a openAMDC "
                    "server configured to handle more than %d "
                    "databases. Exiting\n", server.dbnum);
                exit(1);
            }
            server.dbnum = (int)dbnum;
        } else if (!strncmp(key_buf, "swap_data_version", 17)) { 
            /* Load the swap data version from RocksDB. */
            /* Convert the value and set the swap data version. */
            if (string2ll(val_buf, vlen, &swap_data_version) == 0) goto cleanup;
            setGblVersion(swap_data_version);
        } else if (!strncmp(key_buf, "cuckoo_filter_seed", 18)) {
            cuckooFilterSetHashFunctionSeed((uint8_t *)val_buf);
        } else if (strstr(key_buf, "cuckoo_filter")) {
            /* Load the cuckoo filter from RocksDB for each database. */
            int count;
            long long db;
            sds *argv = sdssplitlen(key_buf, klen, "#", 1, &count);
            if (argv && count == 2) {
                if (string2ll(argv[1], sdslen(argv[1]), &db) == 0) {
                    sdsfreesplitres(argv, count);
                    goto cleanup;
                }
                /* Free the existing cuckoo filter. */
                cuckooFilterFree(&server.swap->cold_filter[db]);
                /* Decode and set the new cuckoo filter. */
                server.swap->cold_filter[db] = cuckooFilterDecodeChunk(val_buf, vlen);
                sdsfreesplitres(argv, count);
            } else {
                serverLog(LL_WARNING, "Failed to decode cuckoo filter");
                sdsfreesplitres(argv, count);
                goto cleanup;
            }
        } else if (strstr(key_buf, "cold_data_size")) {
            /* Load the cold data size from RocksDB for each database. */
            int count;
            long long db;
            sds *argv = sdssplitlen(key_buf, klen, "#", 1, &count);
            if (argv && count == 2) {
                if (string2ll(argv[1], sdslen(argv[1]), &db) == 0) {
                    sdsfreesplitres(argv, count);
                    goto cleanup;
                }
                if (string2ll(val_buf, vlen, &cold_data_size) == 0) {
                    sdsfreesplitres(argv, count);
                    goto cleanup;
                }
                /* Set the cold data size. */
                server.db[db].cold_data_size = cold_data_size;
                sdsfreesplitres(argv, count);
            } else {
                serverLog(LL_WARNING, "Failed to decode cuckoo filter");
                sdsfreesplitres(argv, count);
                goto cleanup;
            }
        } else {
            /* We ignore fields we don't understand. */
            serverLog(LL_WARNING,"Unrecognized rocksdb meta field");
        }
    }

    /* Iterate over each database. */
    for (int i = 0; i < server.dbnum; i++) {
        iter = iterators[DB_CF(i)];
        /* Move the iterator to the first key. */
        rocksdb_iter_seek_to_first(iter);
        /* If the iterator is valid, set no_empty to 1 (indicating the database is empty). */
        db += rocksdb_iter_valid(iter) ? 1 : 0;
    }

    /* Determine the amount of memory to load. */
    mem_toload = server.swap_hotmemory -
                 (getSwapHotmemoryState(&mem_reported,
                                        &mem_used,
                                        NULL,
                                        NULL) == C_OK ? mem_reported : mem_used);
    /* Continue loading until the required memory is loaded. */
    while(db && mem_loaded < mem_toload) {
        dbid = (dist++) % server.dbnum; 
        /* Get the iterator for the current database. */
        iter = iterators[DB_CF(dbid)];

        /* If the iterator is invalid, continue. */
        if (!rocksdb_iter_valid(iter)) continue;

        swapDataRetrieval *r;
        uint64_t version;
        long long expiretime, lfu_freq;
        size_t klen, vlen;
        char *key_buf = (char *)rocksdb_iter_key(iter, &klen);
        char *val_buf = (char *)rocksdb_iter_value(iter, &vlen);
        sds key = sdsnewlen(key_buf, klen);
        robj keyobj, *o;
        initStaticStringObject(keyobj, key);

        /* Record the initial memory usage. */
        delta = zmalloc_used_memory();

        /* Decode the retrieved data， if decoding fails, free the allocated memory and return NULL. */
        if ((r = swapDataDecodeObject(&keyobj, val_buf, vlen)) == NULL) { 
            sdsfree(key);
            goto cleanup;
        } else {
            o = r->val;
            expiretime = r->expiretime;
            lfu_freq = r->lfu_freq;
            version = r->version;
            swapDataRetrievalRelease(r);
        }

        /* If the current server is the master and the object has expired, delete the object from
         * RocksDB and free the object. */
        if (iAmMaster() &&
            expiretime != -1 && expiretime < mstime()) {
            decrRefCount(o);
            rocksdb_delete_cf(server.swap->rocks->db,
                              server.swap->rocks->wopts,
                              server.swap->rocks->cf_handles[DB_CF(dbid)],
                              key,
                              sdslen(key),
                              &err);
            if (err != NULL) {
                serverLog(LL_WARNING, "Rocksdb delete key error, key:%s, err: %s", key, err);
                sdsfree(key);
                goto cleanup;
            }
            /* Delete the key from the cold filter. */
            cuckooFilterDelete(&server.swap->cold_filter[dbid], key, sdslen(key));
            server.db[dbid].cold_data_size--;
            server.db[dbid].stat_swap_in_expired_keys_skipped++;
        } else {
            /* Add the new object in the hash table. */
            int added = dbAddRDBLoad(server.db+dbid,key,o);
            if (!added) {
                serverLog(LL_WARNING,"Rocksdb has duplicated key '%s' in DB %d", key, dbid);
                serverPanic("Duplicated key found in Rocksdb");
            }

            /* Set the expire time if needed. */
            if (expiretime != -1) {
                setExpire(NULL, server.db+dbid, &keyobj, expiretime);
            }

            /* Set usage information (for swap). */
            objectSetLRUOrLFU(o, lfu_freq, -1, LRU_CLOCK(), 1000);

            /* Set the version of the object. */
            setVersion(o, version);

            /* Delete the key from the cuckoo filter. */
            cuckooFilterDelete(&server.swap->cold_filter[dbid], key, sdslen(key));

            /* Decrement the cold data size. */
            server.db[dbid].cold_data_size--;
        }

        /* Calculate the memory used by the loaded object. */
        delta = zmalloc_used_memory() - delta;

        /* Calculate the number of intervals that have passed since the last
         * event processing for both the current loaded memory (`mem_loaded`)
         * and the updated loaded memory (`mem_loaded + delta`). If the updated
         * loaded memory crosses into a new interval, it means enough memory has
         * been loaded to warrant processing events. */
        if (server.loading_process_events_interval_bytes &&
            ((size_t)(mem_loaded + delta))/server.loading_process_events_interval_bytes >
            (size_t)mem_loaded/server.loading_process_events_interval_bytes) {
            /* Report loading progress. */
            loadingProgress(delta);
            /* Process events while blocked. */
            processEventsWhileBlocked(threadId);
            /* Process module loading progress event. */
            processModuleLoadingProgressEvent(0);
        }

        /* Add the memory used by the loaded object to the total memory loaded. */
        mem_loaded += delta;
        /* Increment the count of keys loaded. */
        keys_loaded++;

        /* Move to the next item in the iterator. */
        rocksdb_iter_next(iter);
        
        /* Update dbs count if the current database is empty. */
        if (!rocksdb_iter_valid(iter)) db--;
    }

    /* If the purge rocksdb after load option is enabled. */
    if (server.swap_purge_rocksdb_after_load) {
        /* Iterate over all databases specified by server.dbnum. */
        for (int i = 0; i < server.dbnum; i++) {
            dictIterator *di;
            dictEntry *de;
            redisDb *db = server.db+i;
            dict *d = db->dict;
            if (dictSize(d) == 0) continue;

            /* Iterate over each entry in the dict. */
            di = dictGetIterator(d);
            while((de = dictNext(di)) != NULL) {
                sds key = dictGetKey(de);
                /* Delete the key from RocksDB column family corresponding to the current database. */
                rocksdb_delete_cf(server.swap->rocks->db,
                                  server.swap->rocks->wopts,
                                  server.swap->rocks->cf_handles[DB_CF(i)],
                                  key,
                                  sdslen(key),
                                  &err);
                if (err != NULL) {
                    serverLog(LL_WARNING, "Rocksdb delete key error, key:%s, err: %s", key, err);
                    dictReleaseIterator(di);
                    goto cleanup;
                }
            }
            dictReleaseIterator(di);
        }
    }

    /* Free the iterators array. */
    for (int i = 0; i < server.dbnum+1; i++) {
        iter = iterators[i];
        rocksdb_iter_destroy(iter);
    }
    zfree(iterators);
    return C_OK;

cleanup:
    if (iterators) {
        /* Free the iterators array. */
        for (int i = 0; i < server.dbnum+1; i++) {
            iter = iterators[i];
            if (iter) rocksdb_iter_destroy(iter);
        }
        zfree(iterators);
    }
    return C_ERR;
}

/* Save hot data to RocksDB. */
int swapHotmemorySave(void) {
    if (!server.swap_enabled) return C_ERR;
    
    sds buf = NULL;
    size_t len, seed_size;
    char *cf_buf = NULL;
    char *err = NULL;
    swapDataEntry *entry = NULL;
    sds name = NULL;
    sds cold_data_size = NULL;
    sds swap_data_version = NULL;
    sds dbnum = NULL;
    uint8_t *seed;
    dictEntry *de;
    dictIterator *di = NULL;

    /* Iterate over each database. */
    for (int i = 0; i < server.dbnum; i++) {
        redisDb *db = server.db+i;
        dict *d = db->dict;
        if (dictSize(d) == 0) continue;
        di = dictGetSafeIterator(d);

        /* Iterate this DB writing every entry */
        while((de = dictNext(di)) != NULL) {
            uint64_t version;
            long long expire;
            sds keystr = dictGetKey(de);
            robj *key = createStringObject(keystr, sdslen(keystr));
            robj *o = dictGetVal(de);
            
            version = getVersion(o);
            expire = getExpire(db,key);
            entry = swapDataEntryCreate(SWAP_OUT, i, key, o, expire, version);
            
            /* Encodes an object into a buffer for storage. */
            if ((buf = swapDataEncodeObject(entry)) == NULL) {
                serverLog(LL_WARNING, "Swap data encode object failed, key:%s", (sds)entry->key->ptr);
                goto cleanup;
            }

            /* Add the encoded object to the RocksDB. */
            rocksdb_put_cf(server.swap->rocks->db,
                           server.swap->rocks->wopts,
                           server.swap->rocks->cf_handles[DB_CF(entry->dbid)],
                           entry->key->ptr,
                           sdslen(entry->key->ptr),
                           buf,
                           sdslen(buf),
                           &err);
            if (err != NULL) {
                serverLog(LL_WARNING, "Rocksdb write failed, err:%s", err);
                goto cleanup;
            }

            /* Inserts the key into the Cuckoo filter. */
            cuckooFilterInsert(&server.swap->cold_filter[entry->dbid], entry->key->ptr, sdslen(entry->key->ptr));
            server.db[entry->dbid].cold_data_size++;

            /* Free the encoded buffer. */
            sdsfree(buf); buf = NULL;
            swapDataEntryRelease(entry); entry = NULL;
        }
        dictReleaseIterator(di); di = NULL;
    }

    /* Save the dbnum to RocksDB. */
    name = sdsnew("dbnum");
    dbnum = sdsfromlonglong(server.dbnum);
    rocksdb_put_cf(server.swap->rocks->db,
                   server.swap->rocks->wopts,
                   server.swap->rocks->cf_handles[META_CF],
                   name,
                   sdslen(name),
                   dbnum,
                   sdslen(dbnum),
                   &err);
    if (err != NULL) {
        serverLog(LL_WARNING, "Rocksdb write failed, err:%s", err);
        goto cleanup;
    }
    sdsfree(name); name = NULL;
    sdsfree(dbnum); dbnum = NULL;

    /* Iterate over each database. */
    for (int i = 0; i < server.dbnum; i++) {
        redisDb *db = server.db+i;
        if (db->cold_data_size == 0) continue;

        /* Save the cold data size to RocksDB. */
        name = sdsnew("cold_data_size");
        name = sdscatprintf(name, "#%d", i);
        cold_data_size = sdsfromlonglong(db->cold_data_size);
        rocksdb_put_cf(server.swap->rocks->db,
                       server.swap->rocks->wopts,
                       server.swap->rocks->cf_handles[META_CF],
                       name,
                       sdslen(name),
                       cold_data_size,
                       sdslen(cold_data_size),
                       &err);
        if (err != NULL) {
            serverLog(LL_WARNING, "Rocksdb write failed, err:%s", err);
            goto cleanup;
        }
        sdsfree(name); name = NULL;
        sdsfree(cold_data_size); cold_data_size = NULL;

        /* Save the cuckoo filter to RocksDB. */
        name = sdsnew("cuckoo_filter");
        name = sdscatprintf(name, "#%d", i);
        cf_buf = cuckooFilterEncodeChunk(&server.swap->cold_filter[i], &len);
        rocksdb_put_cf(server.swap->rocks->db,
                       server.swap->rocks->wopts,
                       server.swap->rocks->cf_handles[META_CF],
                       name,
                       sdslen(name),
                       cf_buf,
                       len,
                       &err);
        if (err != NULL) {
            serverLog(LL_WARNING, "Rocksdb write failed, err:%s", err);
            goto cleanup;
        }
        sdsfree(name); name = NULL;
        zfree(cf_buf); cf_buf = NULL;
    }

    /* Save the swap data version to RocksDB. */
    name = sdsnew("swap_data_version");
    swap_data_version = sdsfromlonglong(server.swap->swap_data_version);
    rocksdb_put_cf(server.swap->rocks->db,
                   server.swap->rocks->wopts,
                   server.swap->rocks->cf_handles[META_CF],
                   name,
                   sdslen(name),
                   swap_data_version,
                   sdslen(swap_data_version),
                   &err);
    if (err != NULL) {
        serverLog(LL_WARNING, "Rocksdb write failed, err:%s", err);
        goto cleanup;
    }
    sdsfree(name); name = NULL;
    sdsfree(swap_data_version); swap_data_version = NULL;

    /* Save the cuckoo filter seed to RocksDB. */
    name = sdsnew("cuckoo_filter_seed");
    seed = GetCuckooFilterHashFunctionSeed(&seed_size);
    rocksdb_put_cf(server.swap->rocks->db,
                   server.swap->rocks->wopts,
                   server.swap->rocks->cf_handles[META_CF],
                   name,
                   sdslen(name),
                   (char *)seed,
                   seed_size,
                   &err);
    if (err != NULL) {
        serverLog(LL_WARNING, "Rocksdb write failed, err:%s", err);
        goto cleanup;
    }
    sdsfree(name); name = NULL;

    return C_OK;

cleanup:
    if (di) dictReleaseIterator(di);
    if (entry) swapDataEntryRelease(entry);
    if (dbnum) sdsfree(dbnum);
    if (buf) sdsfree(buf);
    if (cf_buf) zfree(cf_buf);
    if (name) sdsfree(name);
    if (cold_data_size) sdsfree(cold_data_size);
    if (swap_data_version) sdsfree(swap_data_version);
    if (err) zlibc_free(err);
    return C_ERR;
}

/* Mark that we are loading in the global state and setup the fields
 * needed to provide loading stats. */
void startLoadingRocksdb(size_t size) {
    /* Load the DB */
    server.loading = 1;
    server.loading_start_time = time(NULL);
    server.loading_loaded_bytes = 0;
    server.loading_total_bytes = size;
    blockingOperationStarts();

    /* Fire the loading modules start event. */
    moduleFireServerEvent(REDISMODULE_EVENT_LOADING,
                          REDISMODULE_SUBEVENT_LOADING_ROCKSDB_START,
                          NULL);
}

/* Loading finished */
void stopLoadingRocksdb(int success) {
    server.loading = 0;
    blockingOperationEnds();

    /* Fire the loading modules end event. */
    moduleFireServerEvent(REDISMODULE_EVENT_LOADING,
                          success?
                          REDISMODULE_SUBEVENT_LOADING_ENDED:
                          REDISMODULE_SUBEVENT_LOADING_FAILED,
                          NULL);
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

    /* Swap data until we're under the limit. */
    while (mem_freed < (long long)mem_tofree) {
        int k, i;
        robj *bestval = NULL;
        sds bestkey = NULL;
        int bestdbid;
        redisDb *db;
        dict *dict;
        dictEntry *de;
        uint64_t version = 0;
        long long expire = -1;
        swapDataEntry *entry;
        swapPoolEntry *pool = server.swap->pool;

        /* Find the best key to swap. */
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
                    bestval = dictGetVal(de);
                    break;
                } else {
                    /* Ghost... Iterate again. */
                }
            }
        }

        /* Finally swap the selected key. */
        if (bestkey) {
            db = server.db+bestdbid;
            robj *keyobj;
            keyobj = createStringObject(bestkey,sdslen(bestkey));
            /* We compute the amount of memory freed by swap alone.
             * AOF and Output buffer memory will be freed eventually so
             * we only care about memory used by the key space. */
            delta = (long long) zmalloc_used_memory();
            latencyStartMonitor(swap_latency);
            expire = ((de = dictFind(db->expires,bestkey)) == NULL) ? -1 : dictGetSignedIntegerVal(de);
            if (bestval == NULL) {
                decrRefCount(keyobj);
                continue;
            }
            version = getVersion(bestval);
            entry = swapDataEntryCreate(SWAP_OUT, bestdbid, keyobj, bestval, expire, version);
            swapDataEntrySubmit(entry, -1, 1);
            latencyEndMonitor(swap_latency);
            latencyAddSampleIfNeeded("hotmemory-trigger-swap-out", swap_latency);
            delta -= (long long) zmalloc_used_memory();
            mem_freed += delta;
            keys_swapped++;
            server.db[bestdbid].stat_swap_out_keys_total++;
            decrRefCount(keyobj);

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
        while (swapFlushThread(threadId, 0) == C_OK &&
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
    return result;
}

/* Start generating an RDB snapshot for the swap process. */
void swapStartGenerateRDB(void) {
    if (!server.swap_enabled) return;

    /* Create a snapshot from the RocksDB database associated with the swap. */
    server.swap->rocks->snapshot = (rocksdb_snapshot_t *)rocksdb_create_snapshot(server.swap->rocks->db);
    /* Set the created snapshot to the read options for consistent reads. */
    rocksdb_readoptions_set_snapshot(server.swap->rocks->ropts, server.swap->rocks->snapshot);
}

/* Iterates over the RocksDB cold data to generate an RDB or AOF file. */
int swapIterateGenerateRDB(rio *rdb, int rdbflags, int dbid, long key_count, size_t processed, long long info_updated_time) {
    if (!server.swap_enabled) return C_OK;

    swapDataRetrieval *r;
    char *pname = (rdbflags & RDBFLAGS_AOF_PREAMBLE) ? "AOF rewrite" : "RDB";
    /* Create an iterator for the specified column family in the RocksDB database. */
    rocksdb_iterator_t *iter_snapshot =
        rocksdb_create_iterator_cf(server.swap->rocks->db,
                                   server.swap->rocks->ropts,
                                   server.swap->rocks->cf_handles[DB_CF(dbid)]);
    /* Iterate over all key-value pairs in the column family. */
    for (rocksdb_iter_seek_to_first(iter_snapshot);
         rocksdb_iter_valid(iter_snapshot);
         rocksdb_iter_next(iter_snapshot)) {
        robj keyobj, *o;
        uint64_t version;
        long long expiretime, lfu_freq;
        size_t klen, vlen;
        /* Get the key and value from the iterator. */
        char *key_buf = (char *)rocksdb_iter_key(iter_snapshot, &klen);
        char *val_buf = (char *)rocksdb_iter_value(iter_snapshot, &vlen);
        sds key = sdsnewlen(key_buf, klen);
        initStaticStringObject(keyobj, key);
        
        /* Check if the key is present in the Cuckoo filter. */
        if (!cuckooFilterContains(&server.swap->cold_filter[dbid], key, sdslen(key))) {
            sdsfree(key);
            continue;
        }

        /* Decode the retrieved data，if decoding fails, free the allocated memory and return NULL. */
        if ((r = swapDataDecodeObject(&keyobj, val_buf, vlen)) == NULL) { 
            sdsfree(key);
            goto werr;
        } else {
            o = r->val;
            expiretime = r->expiretime;
            lfu_freq = r->lfu_freq;
            version = r->version;
            swapDataRetrievalRelease(r);
        }

        /* Set usage information (for swap). */
        objectSetLRUOrLFU(o, lfu_freq, -1, LRU_CLOCK(), 1000);

        /* Set the version of the object. */
        setVersion(o, version);

        /* Save the key-value pair to the RDB. */
        if (rdbSaveKeyValuePair(rdb, &keyobj, o, expiretime) == -1) {
            sdsfree(key);
            goto werr;
        }

        /* When this RDB is produced as part of an AOF rewrite, move
         * accumulated diff from parent to child while rewriting in
         * order to have a smaller final write. */
        if (rdbflags & RDBFLAGS_AOF_PREAMBLE &&
            rdb->processed_bytes > processed+AOF_READ_DIFF_INTERVAL_BYTES)
        {
            processed = rdb->processed_bytes;
            aofReadDiffFromParent();
        }

        /* Update child info every 1 second (approximately).
         * in order to avoid calling mstime() on each iteration, we will
         * check the diff every 1024 keys */
        if ((key_count++ & 1023) == 0) {
            long long now = mstime();
            if (now - info_updated_time >= 1000) {
                sendChildInfo(CHILD_INFO_TYPE_CURRENT_INFO, key_count, pname);
                info_updated_time = now;
            }
        }
        sdsfree(key);
    }
    /* Destroy the iterator. */
    rocksdb_iter_destroy(iter_snapshot);
    iter_snapshot = NULL;
    return C_OK;

werr:
    /* If an error occurs, destroy the iterator and return an error. */
    if (iter_snapshot)
        rocksdb_iter_destroy(iter_snapshot);
    return C_ERR;
}

/* Stops the RDB generation process by releasing the RocksDB snapshot. */
void swapStopGenerateRDB(void) {
    if (!server.swap_enabled) return;

    /* Release the RocksDB snapshot. */
    rocksdb_release_snapshot(server.swap->rocks->db, server.swap->rocks->snapshot);
    /* Reset the snapshot in the read options. */
    rocksdb_readoptions_set_snapshot(server.swap->rocks->ropts, NULL);
    /* Set the snapshot pointer to NULL. */
    server.swap->rocks->snapshot = NULL;
}

/* Iterates over the RocksDB cold data to generate an AOF file. */
int swapIterateGenerateAppendOnlyFile(rio *aof, int dbid, long key_count, size_t processed, long long updated_time) {
    if (!server.swap_enabled) return C_OK;

    /* Create an iterator for the specified column family in the RocksDB database. */
    rocksdb_iterator_t *iter =
        rocksdb_create_iterator_cf(server.swap->rocks->db,
                                   server.swap->rocks->ropts,
                                   server.swap->rocks->cf_handles[DB_CF(dbid)]);
    /* Iterate over all key-value pairs in the column family. */
    for (rocksdb_iter_seek_to_first(iter);
         rocksdb_iter_valid(iter);
         rocksdb_iter_next(iter)) {
        robj keyobj, *o;
        long long expiretime;
        swapDataRetrieval *r;
        size_t klen, vlen;
        /* Get the key and value from the iterator. */
        char *key_buf = (char *)rocksdb_iter_key(iter, &klen);
        char *val_buf = (char *)rocksdb_iter_value(iter, &vlen);
        sds key = sdsnewlen(key_buf, klen);
        initStaticStringObject(keyobj, key);

        /* Check if the key is present in the Cuckoo filter. */
        if (!cuckooFilterContains(&server.swap->cold_filter[dbid], key, sdslen(key))) {
            sdsfree(key);
            continue;
        }

        /* Decode the retrieved data，if decoding fails, free the allocated memory and return NULL. */
        if ((r = swapDataDecodeObject(&keyobj, val_buf, vlen)) == NULL) { 
            sdsfree(key);
            goto werr;
        } else {
            o = r->val;
            expiretime = r->expiretime;
            swapDataRetrievalRelease(r);
        }

        /* Save the key and associated value */
        if (o->type == OBJ_STRING) {
            /* Emit a SET command */
            char cmd[]="*3\r\n$3\r\nSET\r\n";
            if (rioWrite(aof,cmd,sizeof(cmd)-1) == 0) goto werr;
            /* Key and value */
            if (rioWriteBulkObject(aof,&keyobj) == 0) goto werr;
            if (rioWriteBulkObject(aof,o) == 0) goto werr;
        } else if (o->type == OBJ_LIST) {
            if (rewriteListObject(aof,&keyobj,o) == 0) goto werr;
        } else if (o->type == OBJ_SET) {
            if (rewriteSetObject(aof,&keyobj,o) == 0) goto werr;
        } else if (o->type == OBJ_ZSET) {
            if (rewriteSortedSetObject(aof,&keyobj,o) == 0) goto werr;
        } else if (o->type == OBJ_HASH) {
            if (rewriteHashObject(aof,&keyobj,o) == 0) goto werr;
        } else if (o->type == OBJ_STREAM) {
            if (rewriteStreamObject(aof,&keyobj,o) == 0) goto werr;
        } else if (o->type == OBJ_MODULE) {
            if (rewriteModuleObject(aof,&keyobj,o) == 0) goto werr;
        } else {
            serverPanic("Unknown object type");
        }
        /* Save the expire time */
        if (expiretime != -1) {
            WRAPPER_MUTEX_LOCK(el, &expireLock);
            char cmd[]="*3\r\n$9\r\nPEXPIREAT\r\n";
            if (rioWrite(aof,cmd,sizeof(cmd)-1) == 0) goto werr;
            if (rioWriteBulkObject(aof,&keyobj) == 0) goto werr;
            if (rioWriteBulkLongLong(aof,expiretime) == 0) goto werr;
        }
        /* Read some diff from the parent process from time to time. */
        if (aof->processed_bytes > processed+AOF_READ_DIFF_INTERVAL_BYTES) {
            processed = aof->processed_bytes;
            aofReadDiffFromParent();
        }

        /* Update info every 1 second (approximately).
         * in order to avoid calling mstime() on each iteration, we will
         * check the diff every 1024 keys */
        if ((key_count++ & 1023) == 0) {
            long long now = mstime();
            if (now - updated_time >= 1000) {
                sendChildInfo(CHILD_INFO_TYPE_CURRENT_INFO, key_count, "AOF rewrite");
                updated_time = now;
            }
        }
        sdsfree(key);
    }

    /* Destroy the iterator. */
    rocksdb_iter_destroy(iter);
    iter = NULL;
    return C_OK;

werr:
    /* If an error occurs, destroy the iterator and return an error. */
    if (iter)
        rocksdb_iter_destroy(iter);
    return C_ERR;
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
        swapDataEntrySubmit(e, -1, 0);
    }
}

/* Swap command. */
void swapCommand(client *c) {
    if (!server.swap_enabled) {
        addReplyError(c, "This instance has swap support disabled");
        return;
    }

    robj *o;
    if (!strcasecmp(c->argv[1]->ptr, "where") && c->argc == 3) {
        if ((o = lookupKeyReadWithFlags(c->db, c->argv[2], LOOKUP_NOSWAP|LOOKUP_NONOTIFY|LOOKUP_NOTOUCH))) {
            addReplyLongLong(c, 1);
        } else {
            if (cuckooFilterContains(&server.swap->cold_filter[c->db->id],
                c->argv[2]->ptr, 
                sdslen(c->argv[2]->ptr))) {
                addReplyLongLong(c, 2);
            } else {
                addReplyLongLong(c, 0);
            }
        }
    } else if (!strcasecmp(c->argv[1]->ptr, "in") && c->argc == 3) {
        if ((o = lookupKeyReadWithFlags(c->db, c->argv[2], LOOKUP_NONOTIFY))) {
            addReply(c, shared.ok);
        } else {
            addReply(c, shared.null[c->resp]);
        }
    } else if (!strcasecmp(c->argv[1]->ptr, "out") && c->argc == 3) {
        if ((o = lookupKeyReadWithFlags(c->db, c->argv[2], LOOKUP_NOSWAP|LOOKUP_NONOTIFY)) == NULL) {
            addReply(c, shared.null[c->resp]);
        } else {
            /* Create a swap data entry object, encapsulating
             * the relevant information for the swap operation */
            uint64_t version = getVersion(o);
            long long expire = getExpire(c->db, c->argv[2]);
            swapDataEntry *entry = swapDataEntryCreate(SWAP_OUT, c->db->id, c->argv[2], o, expire, version);
            /* Submit the swap data entry to the swap */
            swapDataEntrySubmit(entry, -1, 1);
            addReply(c, shared.ok);
        }
    } else {
        addReplySubcommandSyntaxError(c);
    }
}
