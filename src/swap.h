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

struct rocks {
  rocksdb_t *db;
  rocksdb_options_t *db_opts;
  rocksdb_readoptions_t *ropts;
  rocksdb_writeoptions_t *wopts;
  rocksdb_column_family_handle_t **cf_handles;
  rocksdb_snapshot_t *snapshot;
};

int rocksInit(void);
int rocksOpen(struct rocks *rocks);
void rocksClose(void);

struct swapState {
    struct rocks *rocks; /* RocksDB data */
    uint64_t swap_data_version;
};

#endif