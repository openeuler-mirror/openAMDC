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

int rocksInit(void) {
    server.swap->rocks = zmalloc(sizeof(struct rocks));
    server.swap->rocks->snapshot = NULL;
    return rocksOpen(server.swap->rocks);
}

int rocksOpen(struct rocks *rocks) {
    char *errs = NULL;
    char **cf_names = zmalloc(sizeof(char *) * server.dbnum);
    rocksdb_options_t **cf_opts = zmalloc(sizeof(rocksdb_options_t *) * server.dbnum);
    rocks->cf_handles = zmalloc(sizeof(rocksdb_column_family_handle_t *) * server.dbnum);
    rocks->db_opts = rocksdb_options_create();
    rocksdb_options_set_create_if_missing(rocks->db_opts, 1);
    rocksdb_options_set_create_missing_column_families(rocks->db_opts, 1);
    rocksdb_options_optimize_for_point_lookup(rocks->db_opts, 1);

    rocksdb_options_set_min_write_buffer_number_to_merge(rocks->db_opts, 2);
    rocksdb_options_set_level0_file_num_compaction_trigger(rocks->db_opts, 2);
    rocksdb_options_set_max_bytes_for_level_base(rocks->db_opts, 256 * MB);
    rocksdb_options_compaction_readahead_size(rocks->db_opts, 2 * 1024 * 1024);

    rocksdb_options_set_max_background_jobs(rocks->db_opts, server.rocksdb_max_background_jobs);
    rocksdb_options_set_max_background_compactions(rocks->db_opts, server.rocksdb_max_background_compactions);
    rocksdb_options_set_max_background_flushes(rocks->db_opts, server.rocksdb_max_background_flushes);
    rocksdb_options_set_max_subcompactions(rocks->db_opts, server.rocksdb_max_subcompactions);
    rocksdb_options_set_max_open_files(rocks->db_opts, server.rocksdb_max_open_files);
    rocksdb_options_set_enable_pipelined_write(rocks->db_opts, server.rocksdb_enable_pipelined_write);

    rocksdb_options_set_max_manifest_file_size(rocks->db_opts, 64 * MB);
    rocksdb_options_set_max_log_file_size(rocks->db_opts, 256 * MB);
    rocksdb_options_set_keep_log_file_num(rocks->db_opts, 12);

    rocks->ropts = rocksdb_readoptions_create();
    rocksdb_readoptions_set_verify_checksums(rocks->ropts, 0);
    rocksdb_readoptions_set_fill_cache(rocks->ropts, 1);

    rocks->wopts = rocksdb_writeoptions_create();
    rocksdb_options_set_WAL_ttl_seconds(rocks->db_opts, server.rocksdb_WAL_ttl_seconds);
    rocksdb_options_set_WAL_size_limit_MB(rocks->db_opts, server.rocksdb_WAL_size_limit_MB);
    rocksdb_options_set_max_total_wal_size(rocks->db_opts, server.rocksdb_max_total_wal_size);

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
    
    rocks->db = rocksdb_open_column_families(rocks->db_opts, ROCKSDB_DIR, server.dbnum, (const char *const *)cf_names,
                                             (const rocksdb_options_t *const *)cf_opts, rocks->cf_handles, &errs);
    if (errs != NULL) {
        serverLog(LL_WARNING, "Rocksdb open column families failed: %s", errs);
        zlibc_free(errs);
        return C_ERR;
    }

    for (int i = 0; i < server.dbnum; i++) {
        sdsfree(cf_names[i]);
        rocksdb_options_destroy(cf_opts[i]);
    }
    zfree(cf_names);
    zfree(cf_opts);
    serverLog(LL_NOTICE, "Rocksdb open success");
    return C_OK;
}

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

void adaptiveLRURelease(adaptiveLRU *al) {
    listRelease(al->warm_aligned_list);
    listRelease(al->warm_unaligned_list);
    listRelease(al->hot_aligned_list);
    listRelease(al->hot_unaligned_list);
    zfree(al);
}

listNode *adaptiveLRUAdd(adaptiveLRU *al, void *val, int to) {
    list *targetList = NULL;

    switch (to) {
        case AL_WARM_ALIGNED_LIST: targetList = al->warm_aligned_list; break;
        case AL_WARM_UNALIGNED_LIST: targetList = al->warm_unaligned_list; break;
        case AL_HOT_ALIGNED_LIST: targetList = al->hot_aligned_list; break;
        case AL_HOT_UNALIGNED_LIST: targetList = al->hot_unaligned_list; break;
        default: serverPanic("Invalid adaptiveLRU type");
    }

    if (listAddNodeHead(targetList, val) == NULL)
        return NULL;
    else
        return listFirst(targetList);
}

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

listNode *adaptiveLRUConvert(adaptiveLRU *al, listNode *node, int *from, int rw) {
    if (rw == AL_READ) {
        switch (*from) {
            case AL_WARM_ALIGNED_LIST:
                return moveNode(al, node, from, AL_HOT_ALIGNED_LIST);
            case AL_HOT_ALIGNED_LIST:
                return moveNode(al, node, from, AL_HOT_ALIGNED_LIST);
            case AL_WARM_UNALIGNED_LIST:
                return moveNode(al, node, from, AL_HOT_UNALIGNED_LIST);
            case AL_HOT_UNALIGNED_LIST:
                return moveNode(al, node, from, AL_HOT_UNALIGNED_LIST);
            default:
                serverPanic("Invalid from value");
        }
    } else if (rw == AL_WRITE) {
        switch (*from) {
            case AL_WARM_ALIGNED_LIST:
                return moveNode(al, node, from, AL_HOT_UNALIGNED_LIST);
            case AL_HOT_ALIGNED_LIST:
                return moveNode(al, node, from, AL_HOT_UNALIGNED_LIST);
            case AL_WARM_UNALIGNED_LIST:
                return moveNode(al, node, from, AL_HOT_UNALIGNED_LIST);
            case AL_HOT_UNALIGNED_LIST:
                return moveNode(al, node, from, AL_HOT_UNALIGNED_LIST);
            default:
                serverPanic("Invalid from value");
        }
    } else {
        serverPanic("Invalid rw value");
    }
}

void adaptiveLRUDel(adaptiveLRU *al, listNode *node, int from) {
    switch(from) {
        case AL_WARM_ALIGNED_LIST: listDelNode(al->warm_aligned_list, node); break;
        case AL_WARM_UNALIGNED_LIST: listDelNode(al->warm_unaligned_list, node); break;
        case AL_HOT_ALIGNED_LIST: listDelNode(al->hot_aligned_list, node); break;
        case AL_HOT_UNALIGNED_LIST: listDelNode(al->hot_unaligned_list, node); break;
        default: serverPanic("Invalid adaptiveLRU type");
    }
}
