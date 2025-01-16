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

#define SWAP_OK 0
#define SWAP_RUNNING 1
#define SWAP_FAIL 2

#define SWAP_HOTMEMORY_FLAG_LFU 1

#define META_CF 0
#define DB_CF(dbid) (dbid+1)

#define ROCKSDB_DIR "rocksdb.dir"

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
} swapDataEntry;

swapDataEntry *swapDataEntryCreate(int intention, int dbid, robj *key, robj *val, long long expiretime);
void swapDataEntryRelease(swapDataEntry *entry);
int swapDataEntrySubmit(swapDataEntry *entry, int idx);

#define SWAP_DATA_ENTRY_BATCH_BUFFER_SIZE 16

typedef struct swapDataEntryBatch {
    struct swapDataEntry *entry_buf[SWAP_DATA_ENTRY_BATCH_BUFFER_SIZE];
    struct swapDataEntry **entries;
    int capacity;
    int count;
    int thread_id;
} swapDataEntryBatch;

swapDataEntryBatch *swapDataEntryBatchCreate(void);
void swapDataEntryBatchRelease(swapDataEntryBatch *eb);
void swapDataEntryBatchAdd(swapDataEntryBatch *eb, swapDataEntry *entry);
int swapDataEntryBatchSubmit(swapDataEntryBatch *eb, int idx, int force);
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

typedef struct swapThread {
    int id;
    pthread_t thread_id;
    pthread_mutex_t lock;
    pthread_cond_t cond;
    list *pending_entries;
} swapThread;

void swapThreadInit(void);
void swapThreadClose(void);

typedef struct swapState {
    rocks *rocks; /* RocksDB data */
    swapThread *swap_threads;
    cuckooFilter *cold_filter;
    swapDataEntryBatch *batch[MAX_THREAD_VAR];
    list *pending_entries[MAX_THREAD_VAR];
    swapPoolEntry *pool;
    redisAtomic uint64_t swap_data_version;
    int hotmemory_policy;
} swapState;

void swapInit(void);
void swapRelease(void);
robj *swapIn(robj* key, int dbid);
void swapOut(robj* key, robj *val, int dbid);
void swapDel(robj* key, int dbid);
int swapFlushThread(int iel, int force);
int swapHotMemoryLoad(void);
int swapHotmemorySave(void);
int performSwapData(void);
int overSwapHotmemoryAfterAlloc(size_t moremem);
void swapStartGenerateRDB(void);
int swapIterateGenerateRDB(rio *rdb, int rdbflags, int dbid, long key_count, size_t processed, long long info_updated_time);
void swapStopGenerateRDB(void);
void swapProcessPendingEntries(int iel);

#endif
