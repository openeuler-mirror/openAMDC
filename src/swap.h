/*
 * Copyright (c) 2024, Apusic
 * This software is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan
 * PSL v2. You may obtain a copy of Mulan PSL v2 at:
 *        http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
 * Mulan PSL v2 for more details.
 */

#ifndef SWAP_H
#define SWAP_H

#include <rocksdb/c.h>
#include "adlist.h"
#include "cuckoo_filter.h"

#define SWAP_IN 0
#define SWAP_OUT 1
#define SWAP_DEL 2

#define SWAP_MAXMEMORY_FLAG_LFU 1

typedef struct rocks {
  rocksdb_t *db;
  rocksdb_options_t *db_opts;
  rocksdb_readoptions_t *ropts;
  rocksdb_writeoptions_t *wopts;
  rocksdb_column_family_handle_t **cf_handles;
  rocksdb_snapshot_t *snapshot;
} rocks;

int rocksInit(void);
int rocksOpen(struct rocks *rocks);
void rocksClose(void);

typedef struct swapDataRetrieval {
    int dbid;
    robj *val;
    long long expiretime;
    long long lfu_freq;
} swapDataRetrieval;

swapDataRetrieval *swapDataRetrievalCreate(int dbid, robj *val, long long expiretime, long long lfu_freq);
void swapDataRetrievalRelease(swapDataRetrieval *r);

typedef struct swapDataEntry {
    int intention;
    int dbid;
    robj *key;
    robj *val;
    long long expiretime;
    uint64_t version;
} swapDataEntry;

swapDataEntry *swapDataEntryCreate(int intention, int dbid, robj *key, robj *val, long long expiretime, uint64_t version);
void swapDataEntryRelease(swapDataEntry *entry);

#define SWAP_DATA_ENTRY_BATCH_BUFFER_SIZE 16

typedef struct swapDataEntryBatch {
    struct swapDataEntry *entry_buf[SWAP_DATA_ENTRY_BATCH_BUFFER_SIZE];
    struct swapDataEntry **entries;
    int capacity;
    int count;
} swapDataEntryBatch;

swapDataEntryBatch *swapDataEntryBatchCreate(void);
void swapDataEntryBatchRelease(swapDataEntryBatch *eb);
void swapDataEntryBatchAdd(swapDataEntryBatch *eb, swapDataEntry *entry);
int swapDataEntryBatchSubmit(swapDataEntryBatch *eb, int idx);
int swapDataEntryBatchProcess(swapDataEntryBatch *eb);

#define SWAP_POOL_SIZE 16
#define SWAP_POOL_CACHED_SDS_SIZE 255

typedef struct swapPoolEntry {
    unsigned long long cost;    /* Object swap cost. */
    sds key;                    /* Key name. */
    sds cached;                 /* Cached SDS object for key name. */
    int dbid;                   /* Key DB number. */
} swapPoolEntry;

swapPoolEntry *swapPoolEntryCreate(void);
void swapPoolEntryRelease(swapPoolEntry *pool);
void swapPoolPopulate(swapPoolEntry *pool, int dbid, dict *sampledict, dict *keydict);

typedef struct swapState {
    rocks *rocks; /* RocksDB data */
    cuckooFilter coldFilter;
    swapDataEntryBatch *batch[MAX_THREAD_VAR];
    list *pending_entries[MAX_THREAD_VAR];
    swapPoolEntry *pool;
    uint64_t swap_data_version;
    int maxmemory_policy;
} swapState;

void swapInit(void);
void swapRelease(void);
robj *swapIn(robj* key, int dbid);
void swapOut(robj* key, robj *val, int dbid);
void swapDel(robj* key, int dbid);
int swapFlushThread(int iel);
void swapProcessPendingEntries(int iel);

#endif
