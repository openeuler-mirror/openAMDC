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

#ifndef __MUTEX_H
#define __MUTEX_H

#include <assert.h>
#include <errno.h>
#include <pthread.h>

typedef int (mutexSkipLock)();

struct mutex {
    char *name;
    int depth;
    pthread_t owner;
    pthread_mutexattr_t attr;
    pthread_mutex_t mutex;
    mutexSkipLock *skipLock;
};

int mutexInit(struct mutex *m, mutexSkipLock *skipLock, char *name);
int mutexLock(struct mutex *m);
int mutexTryLock(struct mutex *m);
int mutexUnlock(struct mutex *m);
int mutexDestroy(struct mutex *m);
int mutexOwnLock(struct mutex *m);

struct wrapperMutex {
    struct mutex *lock;
    int depth;
};

#define WRAPPER_MUTEX_DEFINE(v) \
    __attribute__((__cleanup__(wrapperMutexUnlock))) struct wrapperMutex (v) = {.lock = NULL, .depth = 0}

#define WRAPPER_MUTEX_DEFER_LOCK(v, l) \
    __attribute__((__cleanup__(wrapperMutexUnlock))) struct wrapperMutex (v) = {.lock = (l), .depth = 0}

#define WRAPPER_MUTEX_LOCK(v, l) \
    __attribute__((__cleanup__(wrapperMutexUnlock))) struct wrapperMutex (v) = {.lock = (l), .depth = 0}; wrapperMutexLock(&(v))

#define WRAPPER_MUTEX_NOCLEANUP_LOCK(v, l) \
    struct wrapperMutex (v) = {.lock = (l), .depth = 0}; wrapperMutexLock(&(v))

#define WRAPPER_MUTEX_NOCLEANUP_DEFINE(v, l) \
    struct wrapperMutex (v) = {.lock = (l), .depth = 1};

int wrapperMutexLock(struct wrapperMutex *wm);
int wrapperMutexTryLock(struct wrapperMutex *wm);
int wrapperMutexUnlock(struct wrapperMutex *wm);
int wrapperMutexOwnLock(struct wrapperMutex *wm);

#endif