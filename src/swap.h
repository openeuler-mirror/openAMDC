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

#define AL_LOC_INVALID 0
#define AL_WARM_ALIGNED_LOC 1
#define AL_WARM_UNALIGNED_LOC 2
#define AL_HOT_ALIGNED_LOC 3
#define AL_HOT_UNALIGNED_LOC 4

#define AL_READ 0
#define AL_WRITE 1
typedef struct adaptiveLRU {
    list *warm_aligned_list;
    list *warm_unaligned_list;
    list *hot_aligned_list;
    list *hot_unaligned_list;
} adaptiveLRU;

/* Prototypes */
adaptiveLRU *adaptiveLRUCreate(void);
void adaptiveLRURelease(adaptiveLRU *al);
listNode *adaptiveLRUAdd(adaptiveLRU *al, void *val, int to);
listNode *adaptiveLRUConvert(adaptiveLRU *al, listNode *node, int *from, int rw);
void adaptiveLRUDel(adaptiveLRU *al, listNode *node, int from);

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

swapDataEntry *swapDataEntryCreate(int intention, int dbid, robj *key, long long expiretime);
void swapDataEntryRelease(swapDataEntry *e);

struct swapState {
    rocks *rocks; /* RocksDB data */
    adaptiveLRU *al;
    cuckooFilter *cf;
    list *pending_reqs[MAX_THREAD_VAR];
    uint64_t swap_data_version;
};

void swapInit(void);
void swapRelease(void);
robj* swapIn(int dbid, robj *key);

#endif
