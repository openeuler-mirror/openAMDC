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

/* SDS allocator selection.
 *
 * This file is used in order to change the SDS allocator at compile time.
 * Just define the following defines to what you want to use. Also add
 * the include of your alternate allocator if needed (not needed in order
 * to use the default libc allocator). */

#ifndef __SDS_ALLOC_H__
#define __SDS_ALLOC_H__

#include "zmalloc.h"
#define s_malloc zmalloc
#define s_realloc zrealloc
#define s_trymalloc ztrymalloc
#define s_tryrealloc ztryrealloc
#define s_free zfree
#define s_malloc_usable zmalloc_usable
#define s_realloc_usable zrealloc_usable
#define s_trymalloc_usable ztrymalloc_usable
#define s_tryrealloc_usable ztryrealloc_usable
#define s_free_usable zfree_usable

#endif
