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

#ifndef __TESTHELP_H
#define __TESTHELP_H

int __failed_tests = 0;
int __test_num = 0;
#define test_cond(descr,_c) do { \
    __test_num++; printf("%d - %s: ", __test_num, descr); \
    if(_c) printf("PASSED\n"); else {printf("FAILED\n"); __failed_tests++;} \
} while(0)
#define test_report() do { \
    printf("%d tests, %d passed, %d failed\n", __test_num, \
                    __test_num-__failed_tests, __failed_tests); \
    if (__failed_tests) { \
        printf("=== WARNING === We have failed tests here...\n"); \
        exit(1); \
    } \
} while(0)

#endif
