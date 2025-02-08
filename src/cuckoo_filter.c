/*
 * Copyright Redis Ltd. 2017 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "cuckoo_filter.h"
#include "zmalloc.h"

#include <string.h>
#include <stdio.h>
#include <assert.h>
#include <math.h>


#ifndef CUCKOO_MALLOC
#define CUCKOO_MALLOC zmalloc
#define CUCKOO_CALLOC zcalloc
#define CUCKOO_REALLOC zrealloc
#define CUCKOO_FREE zfree
#endif

static uint8_t cuckoo_hash_function_seed[16];

static int cuckooFilterGrow(cuckooFilter *filter);

static int isPower2(uint64_t num) { return (num & (num - 1)) == 0 && num != 0; }

static uint64_t getNextN2(uint64_t n) {
    n--;
    n |= n >> 1;
    n |= n >> 2;
    n |= n >> 4;
    n |= n >> 8;
    n |= n >> 16;
    n |= n >> 32;
    n++;
    return n;
}

void cuckooFilterSetHashFunctionSeed(uint8_t *seed) {
    memcpy(cuckoo_hash_function_seed,seed,sizeof(cuckoo_hash_function_seed));
}

int cuckooFilterInit(cuckooFilter *filter, uint64_t levelSize, uint16_t bucketSize,
                      uint16_t maxIterations, uint16_t expansion, int lazy) {
    uint64_t capacity = levelSize / sizeof(CuckooFingerprint);
    memset(filter, 0, sizeof(*filter));
    filter->numFilters = 0;
    filter->expansion = getNextN2(expansion);
    filter->bucketSize = bucketSize;
    filter->maxIterations = maxIterations;
    filter->numBuckets = getNextN2(capacity / bucketSize);
    if (filter->numBuckets == 0) {
        filter->numBuckets = 1;
    }
    assert(isPower2(filter->numBuckets));

    if (!lazy && cuckooFilterGrow(filter) != 0) {
        return -1; // LCOV_EXCL_LINE memory failure
    }
    return 0;
}

void cuckooFilterFree(cuckooFilter *filter) {
    for (uint16_t ii = 0; ii < filter->numFilters; ++ii) {
        CUCKOO_FREE(filter->tables[ii].data);
    }
    CUCKOO_FREE(filter->tables);
}

void cuckooFilterClear(cuckooFilter *filter) {
    cuckooFilterFree(filter);
    filter->numFilters = 0;
}

static int cuckooFilterGrow(cuckooFilter *filter) {
    cuckooFilterTable *filtersArray =
        CUCKOO_REALLOC(filter->tables, sizeof(*filtersArray) * (filter->numFilters + 1));

    if (!filtersArray) {
        return -1; // LCOV_EXCL_LINE memory failure
    }
    cuckooFilterTable *table = filtersArray + filter->numFilters;
    size_t growth = pow(filter->expansion, filter->numFilters);
    table->bucketSize = filter->bucketSize;
    table->numBuckets = filter->numBuckets * growth;
    table->data =
        CUCKOO_CALLOC((size_t)table->numBuckets * filter->bucketSize * sizeof(CuckooFingerprint));
    if (!table->data) {
        return -1; // LCOV_EXCL_LINE memory failure
    }

    filter->numFilters++;
    filter->tables = filtersArray;
    return 0;
}

typedef struct {
    uint64_t h1;
    uint64_t h2;
    CuckooFingerprint fp;
} lookupParams;

static uint64_t getAltHash(const cuckooFilter *filter, CuckooFingerprint fp, uint64_t index) {
    return ((uint64_t)(index ^ ((uint64_t)fp * 0x5bd1e995))) & (filter->numBuckets - 1);
}

static void getLookupParams(const cuckooFilter *filter, uint64_t hash, lookupParams *params) {
    params->fp = (hash & 0xFFFFFFFF) & ((1ULL << (sizeof(CuckooFingerprint))*8) - 1);
    params->fp += params->fp == 0;

    params->h1 = (hash >> 32) & (filter->numBuckets - 1);
    params->h2 = getAltHash(filter, params->fp, params->h1);
}

static uint32_t subCFGetIndex(const cuckooFilterTable *table, uint64_t hash) {
    return (hash % table->numBuckets) * table->bucketSize;
}

static CuckooFingerprint *bucketFind(CuckooFingerprint *bucket, uint16_t bucketSize, CuckooFingerprint fp) {
    for (uint16_t ii = 0; ii < bucketSize; ++ii) {
        if (bucket[ii] == fp) {
            return bucket + ii;
        }
    }
    return NULL;
}

static int filterFind(const cuckooFilterTable *filter, const lookupParams *params) {
    uint8_t bucketSize = filter->bucketSize;
    uint64_t loc1 = subCFGetIndex(filter, params->h1);
    uint64_t loc2 = subCFGetIndex(filter, params->h2);
    return bucketFind(&filter->data[loc1], bucketSize, params->fp) != NULL ||
           bucketFind(&filter->data[loc2], bucketSize, params->fp) != NULL;
}

static int bucketDelete(CuckooFingerprint *bucket, uint16_t bucketSize, CuckooFingerprint fp) {
    for (uint16_t ii = 0; ii < bucketSize; ii++) {
        if (bucket[ii] == fp) {
            bucket[ii] = CUCKOO_NULLFP;
            return 1;
        }
    }
    return 0;
}

static int filterDelete(const cuckooFilterTable *filter, const lookupParams *params) {
    uint8_t bucketSize = filter->bucketSize;
    uint64_t loc1 = subCFGetIndex(filter, params->h1);
    uint64_t loc2 = subCFGetIndex(filter, params->h2);
    return bucketDelete(&filter->data[loc1], bucketSize, params->fp) ||
           bucketDelete(&filter->data[loc2], bucketSize, params->fp);
}

static int cuckooFilterCheckFP(const cuckooFilter *filter, const lookupParams *params) {
    for (uint16_t ii = 0; ii < filter->numFilters; ++ii) {
        if (filterFind(&filter->tables[ii], params)) {
            return 1;
        }
    }
    return 0;
}

int cuckooFilterContains(const cuckooFilter *filter, const char *key, size_t klen) {
    lookupParams params;
     uint64_t hash = CUCKOO_GEN_HASH(key, klen);
    getLookupParams(filter, hash, &params);
    return cuckooFilterCheckFP(filter, &params);
}

static uint16_t bucketCount(const CuckooFingerprint *bucket, uint16_t bucketSize, CuckooFingerprint fp) {
    uint16_t ret = 0;
    for (uint16_t ii = 0; ii < bucketSize; ++ii) {
        if (bucket[ii] == fp) {
            ret++;
        }
    }
    return ret;
}

static uint64_t subFilterCount(const cuckooFilterTable *table, const lookupParams *params) {
    uint8_t bucketSize = table->bucketSize;
    uint64_t loc1 = subCFGetIndex(table, params->h1);
    uint64_t loc2 = subCFGetIndex(table, params->h2);

    return bucketCount(&table->data[loc1], bucketSize, params->fp) +
           bucketCount(&table->data[loc2], bucketSize, params->fp);
}

uint64_t cuckooFilterCount(const cuckooFilter *filter, const char *key, size_t klen) {
    lookupParams params;
    uint64_t hash = CUCKOO_GEN_HASH(key, klen);
    getLookupParams(filter, hash, &params);
    uint64_t ret = 0;
    for (uint16_t ii = 0; ii < filter->numFilters; ++ii) {
        ret += subFilterCount(&filter->tables[ii], &params);
    }
    return ret;
}

int cuckooFilterDelete(cuckooFilter *filter, const char *key, size_t klen) {
    lookupParams params;
    uint64_t hash = CUCKOO_GEN_HASH(key, klen);
    getLookupParams(filter, hash, &params);
    for (uint16_t ii = filter->numFilters; ii > 0; --ii) {
        if (filterDelete(&filter->tables[ii - 1], &params)) {
            filter->numItems--;
            filter->numDeletes++;
            if (filter->numFilters > 1 && filter->numDeletes > (double)filter->numItems * 0.10) {
                cuckooFilterCompact(filter, false);
            }
            return 1;
        }
    }
    return 0;
}

static CuckooFingerprint *bucketFindAvailable(CuckooFingerprint *bucket, uint16_t bucketSize) {
    for (uint16_t ii = 0; ii < bucketSize; ++ii) {
        if (bucket[ii] == CUCKOO_NULLFP) {
            return &bucket[ii];
        }
    }
    return NULL;
}

static CuckooFingerprint *filterFindAvailable(cuckooFilterTable *filter, const lookupParams *params) {
    CuckooFingerprint *slot;
    uint8_t bucketSize = filter->bucketSize;
    uint64_t loc1 = subCFGetIndex(filter, params->h1);
    uint64_t loc2 = subCFGetIndex(filter, params->h2);
    if ((slot = bucketFindAvailable(&filter->data[loc1], bucketSize)) ||
        (slot = bucketFindAvailable(&filter->data[loc2], bucketSize))) {
        return slot;
    }
    return NULL;
}

static cuckooInsertStatus filterKOInsert(cuckooFilter *filter, cuckooFilterTable *table,
                                          const lookupParams *params);

static cuckooInsertStatus cuckooFilterInsertFP(cuckooFilter *filter, const lookupParams *params) {
    for (uint16_t ii = filter->numFilters; ii > 0; --ii) {
        CuckooFingerprint *slot = filterFindAvailable(&filter->tables[ii - 1], params);
        if (slot) {
            *slot = params->fp;
            filter->numItems++;
            return CUCKOO_FILTER_INSERTED;
        }
    }

    // No space. Time to evict!
    if (filter->numFilters > 0) {
        cuckooInsertStatus status =
            filterKOInsert(filter, &filter->tables[filter->numFilters - 1], params);
        if (status == CUCKOO_FILTER_INSERTED) {
            filter->numItems++;
            return CUCKOO_FILTER_INSERTED;
        }
    }

    if (filter->expansion == 0) {
        return CUCKOO_FILTER_NOSPACE;
    }

    if (cuckooFilterGrow(filter) != 0) {
        return CUCKOO_FILTER_MEMALLOCFAILED;
    }

    // Try to insert the filter again
    return cuckooFilterInsertFP(filter, params);
}

cuckooInsertStatus cuckooFilterInsert(cuckooFilter *filter, const char *key, size_t klen) {
    lookupParams params;
    uint64_t hash = CUCKOO_GEN_HASH(key, klen);
    getLookupParams(filter, hash, &params);
    return cuckooFilterInsertFP(filter, &params);
}

cuckooInsertStatus cuckooFilterInsertUnique(cuckooFilter *filter, const char *key, size_t klen) {
    lookupParams params;
    uint64_t hash = CUCKOO_GEN_HASH(key, klen);
    getLookupParams(filter, hash, &params);
    if (cuckooFilterCheckFP(filter, &params)) {
        return CUCKOO_FILTER_EXISTS;
    }
    return cuckooFilterInsertFP(filter, &params);
}

static void swapFPs(CuckooFingerprint *a, CuckooFingerprint *b) {
    CuckooFingerprint temp = *a;
    *a = *b;
    *b = temp;
}

static cuckooInsertStatus filterKOInsert(cuckooFilter *filter, cuckooFilterTable *table,
                                          const lookupParams *params) {
    uint16_t maxIterations = filter->maxIterations;
    uint32_t numBuckets = table->numBuckets;
    uint16_t bucketSize = filter->bucketSize;
    CuckooFingerprint fp = params->fp;

    uint16_t counter = 0;
    uint32_t victimIx = 0;
    uint32_t ii = params->h1 % numBuckets;

    while (counter++ < maxIterations) {
        CuckooFingerprint *bucket = &table->data[ii * bucketSize];
        swapFPs(bucket + victimIx, &fp);
        ii = getAltHash(filter, fp, ii);
        // Insert the new item in potentially the same bucket
        CuckooFingerprint *empty = bucketFindAvailable(&table->data[ii * bucketSize], bucketSize);
        if (empty) {
            *empty = fp;
            return CUCKOO_FILTER_INSERTED;
        }
        victimIx = (victimIx + 1) % bucketSize;
    }

    // If we weren't able to insert, we roll back and try to insert new element in new filter
    counter = 0;
    while (counter++ < maxIterations) {
        victimIx = (victimIx + bucketSize - 1) % bucketSize;
        ii = getAltHash(filter, fp, ii);
        CuckooFingerprint *bucket = &table->data[ii * bucketSize];
        swapFPs(bucket + victimIx, &fp);
    }

    return CUCKOO_FILTER_NOSPACE;
}

#define RELOC_EMPTY 0
#define RELOC_OK 1
#define RELOC_FAIL -1

/**
 * Attempt to move a slot from one bucket to another filter
 */
static int relocateSlot(cuckooFilter *filter, CuckooFingerprint *bucket, uint16_t filterIx, uint64_t bucketIx,
                        uint16_t slotIx) {
    lookupParams params = {0};
    if ((params.fp = bucket[slotIx]) == CUCKOO_NULLFP) {
        // Nothing in this slot.
        return RELOC_EMPTY;
    }

    // Because We try to insert in sub filter with less or equal number of
    // buckets, our current fingerprint is sufficient
    params.h1 = bucketIx;
    params.h2 = getAltHash(filter, params.fp, bucketIx);

    // Look at all the prior filters and attempt to find a home
    for (uint16_t ii = 0; ii < filterIx; ++ii) {
        CuckooFingerprint *slot = filterFindAvailable(&filter->tables[ii], &params);
        if (slot) {
            *slot = params.fp;
            bucket[slotIx] = CUCKOO_NULLFP;
            return RELOC_OK;
        }
    }
    return RELOC_FAIL;
}

/**
 * Attempt to strip a single filter moving it down a slot
 */
static int cuckooFilterCompactSingle(cuckooFilter *filter, uint16_t filterIx) {
    cuckooFilterTable *table = &filter->tables[filterIx];
    CuckooFingerprint *finger = table->data;
    int rv = RELOC_OK;

    for (uint64_t bucketIx = 0; bucketIx < table->numBuckets; ++bucketIx) {
        for (uint16_t slotIx = 0; slotIx < table->bucketSize; ++slotIx) {
            int status = relocateSlot(filter, &finger[bucketIx * table->bucketSize], filterIx,
                                      bucketIx, slotIx);
            if (status == RELOC_FAIL) {
                rv = RELOC_FAIL;
            }
        }
    }
    // we free a filter only if it the latest one
    if (rv == RELOC_OK && filterIx == filter->numFilters - 1) {
        CUCKOO_FREE(filter);
        filter->numFilters--;
    }
    return rv;
}

/**
 * Attempt to move elements to older filters. If latest filter is emptied, it is freed.
 * `bool` determines whether to continue iteration on other filters once a filter cannot
 * be freed and therefore following filter cannot be freed either.
 */
void cuckooFilterCompact(cuckooFilter *filter, bool cont) {
    for (uint64_t ii = filter->numFilters; ii > 1; --ii) {
        if (cuckooFilterCompactSingle(filter, ii - 1) == RELOC_FAIL && !cont) {
            // if compacting failed, stop as lower filters cannot be freed.
            break;
        }
    }
    filter->numDeletes = 0;
}

void cuckooFilterGetStat(const cuckooFilter *filter, cuckooFilterStat *stat) {
    size_t total_slots = 0;
    memset(stat,0,sizeof(cuckooFilterStat));
    stat->numFilters = filter->numFilters;
    stat->numItems = filter->numItems;
    stat->numDeletes = filter->numDeletes;
    stat->bucketSize = filter->bucketSize;
    stat->maxIterations = filter->maxIterations;
    for (int i = 0; i < filter->numFilters; i++) {
        cuckooFilterTable *table = filter->tables+i;
        size_t slots = table->bucketSize*table->numBuckets;
        stat->used_memory += sizeof(CuckooFingerprint)*slots;
        total_slots += slots;
    }
    stat->load_factor = (double)stat->numItems / total_slots;
}

/**
 * Encodes a cuckoo filter into a contiguous memory buffer
 */
char *cuckooFilterEncodeChunk(cuckooFilter *filter, size_t *len) {
    size_t cuckooFilterTableSize = filter->numBuckets * filter->bucketSize * sizeof(CuckooFingerprint);
    *len = sizeof(cuckooFilterHeader) + filter->numFilters * cuckooFilterTableSize;
    char *buf = CUCKOO_MALLOC(*len);

    cuckooFilterHeader header = (cuckooFilterHeader) {
        .numBuckets = filter->numBuckets,
        .numItems = filter->numItems,
        .numDeletes = filter->numDeletes,
        .numFilters = filter->numFilters,
        .bucketSize = filter->bucketSize,
        .maxIterations = filter->maxIterations,
        .expansion = filter->expansion};
    memcpy(buf, &header, sizeof(cuckooFilterHeader));

    for (uint16_t i = 0; i < filter->numFilters; i++) {
        size_t offset = sizeof(cuckooFilterHeader) + i * cuckooFilterTableSize;
        cuckooFilterTable *table = filter->tables + i;
        memcpy(buf + offset, table->data, cuckooFilterTableSize);
    }

    return buf;
}

/**
 * Decodes a cuckoo filter from a contiguous memory buffer
 */
cuckooFilter cuckooFilterDecodeChunk(const char *buf, size_t len) {
    assert(len >= sizeof(cuckooFilterHeader));
    cuckooFilterHeader header;
    memcpy(&header, buf, sizeof(cuckooFilterHeader));

    cuckooFilter filter;
    filter.numBuckets = header.numBuckets;
    filter.numItems = header.numItems;
    filter.numDeletes = header.numDeletes;
    filter.numFilters = header.numFilters;
    filter.bucketSize = header.bucketSize;
    filter.maxIterations = header.maxIterations;
    filter.expansion = header.expansion;
    filter.tables = CUCKOO_MALLOC(sizeof(cuckooFilterTable) * filter.numFilters);

    size_t cuckooFilterTableSize = filter.numBuckets * filter.bucketSize * sizeof(CuckooFingerprint);
    for (uint16_t i = 0; i < filter.numFilters; i++) {
        filter.tables->data = CUCKOO_MALLOC(cuckooFilterTableSize);
        size_t offset = sizeof(cuckooFilterHeader) + i * cuckooFilterTableSize;
        cuckooFilterTable *table = filter.tables + i;
        table->bucketSize = filter.bucketSize;
        table->numBuckets = filter.numBuckets;
        memcpy(table->data, buf + offset, cuckooFilterTableSize);
    }

    return filter;
}

// Returns 0 on success
int cuckooFilterValidateIntegrity(const cuckooFilter *filter) {
    if (filter->bucketSize == 0 || filter->bucketSize > CF_MAX_BUCKET_SIZE ||
        filter->numBuckets == 0 || filter->numBuckets > CF_MAX_NUM_BUCKETS ||
        filter->numFilters == 0 || filter->maxIterations == 0 || !isPower2(filter->numBuckets) ) {
        return 1;
    }

    return 0;
}


#ifdef REDIS_TEST

#define UNUSED(x) (void)(x)
int cuckooFilterTest(int argc, char **argv, int accurate) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(accurate);
    uint64_t capacity = 1000000;
    cuckooFilter ck;
    cuckooFilterInit(&ck, capacity * sizeof(CuckooFingerprint), CF_DEFAULT_BUCKETSIZE, 500, 1, 0);
    assert(ck.numItems == 0);
    assert(ck.numFilters == 1);

    size_t false_positive = 0, total = 0;

    for (size_t i = 0; i < capacity; i++) {
        assert(cuckooFilterInsert(&ck, (char *)&i, sizeof(size_t)) == CUCKOO_FILTER_INSERTED);
    }

    for (size_t i = 0; i < capacity; i++) {
        assert(cuckooFilterContains(&ck, (char *)&i, sizeof(size_t)));
    }

    for (size_t i = capacity; i < capacity * 2; i++) {
        if (cuckooFilterContains(&ck, (char *)&i, sizeof(size_t))) {
            false_positive++;
        }
        total++;
    }

    double fp_rate = (double)false_positive / total;

    if (sizeof(CuckooFingerprint) == 1) {
        assert(fp_rate < 0.03);
    } else if (sizeof(CuckooFingerprint) == 2) {
        assert(fp_rate < 0.0007);
    } else if (sizeof(CuckooFingerprint) == 4) {
        assert(fp_rate < 0.00001);
    }

    cuckooFilterFree(&ck);
    // printf("capacity=%llu, fp=%ld, fp_rate=%f%%\n", capacity, false_positive, fp_rate * 100);
    return 0;
}

#endif