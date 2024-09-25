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

/* Every time the openAMDC Git SHA1 or Dirty status changes only this small
 * file is recompiled, as we access this information in all the other
 * files using this functions. */

#include <string.h>
#include <stdio.h>

#include "release.h"
#include "version.h"
#include "crc64.h"

char *redisGitSHA1(void) {
    return OPENAMDC_GIT_SHA1;
}

char *redisGitDirty(void) {
    return OPENAMDC_GIT_DIRTY;
}

uint64_t redisBuildId(void) {
    char *buildid = OPENAMDC_VERSION OPENAMDC_BUILD_ID OPENAMDC_GIT_DIRTY OPENAMDC_GIT_SHA1;

    return crc64(0,(unsigned char*)buildid,strlen(buildid));
}

/* Return a cached value of the build string in order to avoid recomputing
 * and converting it in hex every time: this string is shown in the INFO
 * output that should be fast. */
char *redisBuildIdString(void) {
    static char buf[32];
    static int cached = 0;
    if (!cached) {
        snprintf(buf,sizeof(buf),"%llx",(unsigned long long) redisBuildId());
        cached = 1;
    }
    return buf;
}
