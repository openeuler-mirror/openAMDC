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

#ifndef CRCSPEED_H
#define CRCSPEED_H

#include <inttypes.h>
#include <stdio.h>

typedef uint64_t (*crcfn64)(uint64_t, const void *, const uint64_t);
typedef uint16_t (*crcfn16)(uint16_t, const void *, const uint64_t);

/* CRC-64 */
void crcspeed64little_init(crcfn64 fn, uint64_t table[8][256]);
void crcspeed64big_init(crcfn64 fn, uint64_t table[8][256]);
void crcspeed64native_init(crcfn64 fn, uint64_t table[8][256]);

uint64_t crcspeed64little(uint64_t table[8][256], uint64_t crc, void *buf,
                          size_t len);
uint64_t crcspeed64big(uint64_t table[8][256], uint64_t crc, void *buf,
                       size_t len);
uint64_t crcspeed64native(uint64_t table[8][256], uint64_t crc, void *buf,
                          size_t len);

/* CRC-16 */
void crcspeed16little_init(crcfn16 fn, uint16_t table[8][256]);
void crcspeed16big_init(crcfn16 fn, uint16_t table[8][256]);
void crcspeed16native_init(crcfn16 fn, uint16_t table[8][256]);

uint16_t crcspeed16little(uint16_t table[8][256], uint16_t crc, void *buf,
                          size_t len);
uint16_t crcspeed16big(uint16_t table[8][256], uint16_t crc, void *buf,
                       size_t len);
uint16_t crcspeed16native(uint16_t table[8][256], uint16_t crc, void *buf,
                          size_t len);
#endif
