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

#include "mutex.h"

/* Initializes a mutex structure with specified attributes */
int mutexInit(struct mutex *m, char *name) {
    int ret = 0;
    m->name = name;
    m->depth = 0;
    m->owner = 0;
    
    /* Initialize mutex attribute object */
    if ((ret = pthread_mutexattr_init(&m->attr)) != 0) return ret;

    /* Set the mutex attributes to be private to the process */
    if ((ret = pthread_mutexattr_setpshared(&m->attr, PTHREAD_PROCESS_PRIVATE)) != 0) {
        pthread_mutexattr_destroy(&m->attr);
        return ret;
    }

    /* Initialize the mutex itself using the configured attributes */
    if ((ret = pthread_mutex_init(&m->mutex, &m->attr)) != 0) {
        pthread_mutexattr_destroy(&m->attr);
        return ret;
    }

    return ret;
}

/* Locks a mutex, handling recursive locks by a single thread */
int mutexLock(struct mutex *m) {
    int ret = 0;

    /* Identify the current thread */
    pthread_t self = pthread_self();

    /* If the current thread already owns the mutex, increment
     * the depth and return */
    if (m->owner == self) {
        m->depth++;
        return ret;
    }

    /* Attempt to lock the mutex using pthread_mutex_lock */
    if ((ret = pthread_mutex_lock(&m->mutex)) != 0) return ret;
    
    /* If this is the first lock, set the owner of the mutex */
    if (m->depth == 0)
        m->owner = self;

    /* Increment the lock depth since the lock was successfully
     * acquired */
    m->depth++;

    return ret;
}

/* Attempts to lock a mutex, supporting recursive locking by the
 * same thread */
int mutexTryLock(struct mutex *m) {
    int ret = 0;

    /* Identify the current thread */
    pthread_t self = pthread_self();

    /* Increment depth and return if the mutex is already owned
     * by the current thread */
    if (m->owner == self) {
        m->depth++;
        return ret;
    }

    /* Try to lock the mutex */
    if ((ret = pthread_mutex_trylock(&m->mutex)) != 0)
        return ret;

    /* If this is the first acquisition of the lock, record
     * the owning thread */
    if (m->depth == 0)
        m->owner = self;
    
    /* Increment the lock depth as the mutex is now held */
    m->depth++;
    
    return ret;
}

/* Unlocks a mutex */
int mutexUnlock(struct mutex *m) {
    /* If the lock depth is zero, the mutex is considered not locked,
     * return ENOLCK error */
    if (m->depth == 0)
        return ENOLCK;

    /* Verify that the calling thread is the owner of the mutex, 
     * if not, return EPERM error */
    if (m->owner != pthread_self())
        return EPERM;

    /* Decrement the lock depth */
    m->depth--;

    /* If the depth reaches zero after decrementing, the mutex
     * is fully unlocked */
    if (m->depth == 0) {
        m->owner = 0;
        return pthread_mutex_unlock(&m->mutex);
    }

    return 0;
}

/* Destroys a mutex and frees associated resources */
int mutexDestroy(struct mutex *m) {
    int ret = 0;

    /* Destroy the mutex attributes. If this operation fails,
     * return the error immediately */
    if ((ret = pthread_mutexattr_destroy(&m->attr)) != 0) return ret;

    /* Destroy the mutex itself. If this operation fails,
     * return the error */
    if ((ret = pthread_mutex_destroy(&m->mutex)) != 0) return ret;

    return ret;
}

/* Checks if the current thread owns the mutex */
int mutexOwnLock(struct mutex *m) {
    return m->owner == pthread_self();
}

/* Wraps a mutex lock operation, tracking lock depth for a wrapper
 * mutex structure */
int wrapperMutexLock(struct wrapperMutex *wm) {
    assert(wm->lock);
    int ret = mutexLock(wm->lock);
    if (ret == 0)
        wm->depth++;
    return ret;
}

/* Attempts to lock a mutex, wrapped for tracking lock depth */
int wrapperMutexTryLock(struct wrapperMutex *wm) {
    assert(wm->lock);
    int ret = mutexTryLock(wm->lock);
    if (ret == 0)
        wm->depth++;
    return ret;
}


/* Unlocks a mutex and decrements the tracked lock depth within a
 * wrapper mutex structure */
int wrapperMutexUnlock(struct wrapperMutex *wm) {
    int ret = 0;
    if (wm->lock == NULL || wm->depth == 0)
        return ret;
    ret = mutexUnlock(wm->lock);
    if (ret == 0)
        wm->depth--;
    return ret;
}

/* Determines if the current thread owns the lock of a wrapper mutex */
int wrapperMutexOwnLock(struct wrapperMutex *wm) {
    assert(wm->lock);
    return mutexOwnLock(wm->lock);
}