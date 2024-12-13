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

#ifndef __CUCKOO_FILTER_H
#define __CUCKOO_FILTER_H

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

#define CUCKOO_BKTSIZE 2
#define CUCKOO_NULLFP 0

typedef uint16_t CuckooFingerprint;

#define CF_DEFAULT_MAX_ITERATIONS 20
#define CF_DEFAULT_BUCKETSIZE 4
#define CF_DEFAULT_EXPANSION 1
#define CF_MAX_EXPANSION 32768
#define CF_MAX_ITERATIONS 65535
#define CF_MAX_BUCKET_SIZE 255                     // 8 bits, see struct SubCF
#define CF_MAX_NUM_BUCKETS (0x00FFFFFFFFFFFFFFULL) // 56 bits, see struct SubCF
#define CF_MAX_NUM_FILTERS (UINT16_MAX)            // 16 bits, see struct CuckooFilter

typedef struct cuckooFilterStat {
    size_t numItems;
    size_t numDeletes;
    size_t used_memory;
    uint16_t numFilters;
    uint16_t bucketSize;
    uint16_t maxIterations;
    double load_factor;
} cuckooFilterStat;

typedef struct {
    uint64_t numBuckets : 56;
    uint64_t bucketSize : 8;
    CuckooFingerprint *data;
} cuckooFilterTable;

typedef struct cuckooFilter {
    uint64_t numBuckets;
    uint64_t numItems;
    uint64_t numDeletes;
    uint16_t numFilters;
    uint16_t bucketSize;
    uint16_t maxIterations;
    uint16_t expansion;
    cuckooFilterTable *tables;
} cuckooFilter;

uint64_t siphash(const uint8_t *in, const size_t inlen, const uint8_t *k);
#define CUCKOO_GEN_HASH(s, n) siphash((const uint8_t *)s,n,cuckoo_hash_function_seed);

typedef enum {
    CUCKOO_FILTER_INSERTED = 1,
    CUCKOO_FILTER_EXISTS = 0,
    CUCKOO_FILTER_NOSPACE = -1,
    CUCKOO_FILTER_MEMALLOCFAILED = -2
} cuckooInsertStatus;

int cuckooFilterInit(cuckooFilter *filter, uint64_t capacity, uint16_t bucketSize,
                      uint16_t maxIterations, uint16_t expansion);
void cuckooFilterFree(cuckooFilter *filter);
cuckooInsertStatus cuckooFilterInsertUnique(cuckooFilter *filter, const char *key, size_t klen);
cuckooInsertStatus cuckooFilterInsert(cuckooFilter *filter, const char *key, size_t klen);
int cuckooFilterDelete(cuckooFilter *filter, const char *key, size_t klen);
int cuckooFilterContains(const cuckooFilter *filter, const char *key, size_t klen);
uint64_t cuckooFilterCount(const cuckooFilter *filter, const char *key, size_t klen);
void cuckooFilterCompact(cuckooFilter *filter, bool cont);
void cuckooFilterGetStat(const cuckooFilter *filter, cuckooFilterStat *stat);
int cuckooFilterValidateIntegrity(const cuckooFilter *filter);
void cuckooFilterSetHashFunctionSeed(uint8_t *seed);

#ifdef REDIS_TEST
int cuckooFilterTest(int argc, char *argv[], int accurate);
#endif

#endif