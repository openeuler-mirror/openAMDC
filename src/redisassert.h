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

#ifndef __REDIS_ASSERT_H__
#define __REDIS_ASSERT_H__

#include "config.h"

#define assert(_e) (likely((_e))?(void)0 : (_serverAssert(#_e,__FILE__,__LINE__),redis_unreachable()))
#define panic(...) _serverPanic(__FILE__,__LINE__,__VA_ARGS__),redis_unreachable()

void _serverAssert(char *estr, char *file, int line);
void _serverPanic(const char *file, int line, const char *msg, ...);

#endif
