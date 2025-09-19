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
#include <sched.h>
#include <unistd.h>
#include <limits.h>
#include <linux/futex.h>
#include <sys/syscall.h>

#ifdef USE_SPINLOCK

#ifndef __GNUC__
    #define likely(x) (x)
    #define unlikely(x) (x)
#else
    #define likely(x) __builtin_expect(!!(x), 1)
    #define unlikely(x) __builtin_expect(!!(x), 0)
#endif

/* Acquires a fair spinlock, always returns success */
static int spinLock(uint16_t *serving, uint16_t *next) {
    /* Load expected using RELAXED ordering */
    unsigned expected = __atomic_fetch_add(next, 1, __ATOMIC_RELAXED);
    /* Use ACQUIRE to establish synchronization with previous unlock's RELEASE */
    uint16_t current = __atomic_load_n(serving, __ATOMIC_ACQUIRE);
    /* Fast path check try to avoid entering the loop if possible */
    if (likely(current == expected)) {
        /* Success */
        return 0;
    }

    /* Slow path */
    while (1) {
        /* We're in a spin loop and will eventually see the update */
        current = __atomic_load_n(serving, __ATOMIC_RELAXED);
        if (likely(current == expected)) {
            /* Ensure all critical section loads after this are visible and
             * synchronize with the previous unlock's RELEASE operation */
            __atomic_thread_fence(__ATOMIC_ACQUIRE);
            break;
        }

        /* Architecture-specific pause instruction to reduce power
         * consumption during spin */
        #if defined(__i386__) || defined(__x86_64__)
            __asm__ __volatile__("pause");
        #elif defined(__aarch64__)
            __asm__ __volatile__("yield");
        #elif defined(__powerpc__)
            __asm__ __volatile__("or 27,27,27");
        #else
            /* Generic compiler barrier to prevent instruction reordering */
            __asm__ __volatile__("" ::: "memory");
        #endif
    }

    /* Success */
    return 0;
}

/* Attempts to acquire a fair spinlock without blocking */
static int spinTryLock(uint16_t *serving, uint16_t *next) {
    /* Load current serving with ACQUIRE */
    uint16_t current = __atomic_load_n(serving, __ATOMIC_ACQUIRE);
    /* Load next with RELAXED, we just need a snapshot for comparison */
    uint16_t expected = __atomic_load_n(next, __ATOMIC_RELAXED);
    /* "unlikely" helps compiler optimization */
    if (unlikely(current != expected)) {
        /* Failure */
        return EBUSY;
    };

    /* Attempt to acquire lock with CAS operation:
     * - Success: ACQUIRE ordering ensures critical section visibility
     * - Failure: RELAXED since we don't need to synchronize a failed attempt */
    if (likely(__atomic_compare_exchange_n(
        next, &expected, current + 1, 0, __ATOMIC_ACQUIRE, __ATOMIC_RELAXED
    ))) {
        /* Success */
        return 0;
    }

    /* Failure */
    return EBUSY;
}

/* Releases a spinlock, always returns success */
static int spinUnlock(uint16_t *serving) {
    /* Increment serving with RELEASE ordering to ensure all
     * critical section stores are visible to next lock acquirer */
    __atomic_add_fetch(serving, 1, __ATOMIC_RELEASE);
    /* Success */
    return 0;
}

/* Initializes a mutex structure with specified attributes */
int mutexInit(struct mutex *m, mutexSkipLock *skipLock, char *name) {
    m->name = name;
    m->depth = 0;
    m->owner = 0;
    m->serving = 0;
    m->next = 0;
    m->skipLock = skipLock;
    return 0;
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
    if ((ret = spinLock(&m->serving, &m->next)) != 0) return ret;
    
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
    if ((ret = spinTryLock(&m->serving, &m->next)) != 0)
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
        return spinUnlock(&m->serving);
    }

    return 0;
}

/* Destroys a mutex and frees associated resources */
int mutexDestroy(struct mutex *m) {
    m->depth = 0;
    m->owner = 0;
    m->serving = 0;
    m->next = 0;
    return 0;
}

/* Checks if the current thread owns the mutex */
int mutexOwnLock(struct mutex *m) {
    return m->owner == pthread_self();
}

#else

/* Initializes a mutex structure with specified attributes */
int mutexInit(struct mutex *m, mutexSkipLock *skipLock, char *name) {
    int ret = 0;
    m->name = name;
    m->depth = 0;
    m->owner = 0;
    m->skipLock = skipLock;
    
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

#endif

/* Wraps a mutex lock operation, tracking lock depth for a wrapper
 * mutex structure */
int wrapperMutexLock(struct wrapperMutex *wm) {
    if (wm->lock == NULL) return 0;
    if (wm->lock->skipLock != NULL && wm->lock->skipLock()) return 0;
    int ret = mutexLock(wm->lock);
    if (ret == 0)
        wm->depth++;
    return ret;
}

/* Attempts to lock a mutex, wrapped for tracking lock depth */
int wrapperMutexTryLock(struct wrapperMutex *wm) {
    if (wm->lock == NULL) return 0;
    if (wm->lock->skipLock != NULL && wm->lock->skipLock()) return 0;
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
    if (wm->lock->skipLock != NULL && wm->lock->skipLock()) return ret;
    ret = mutexUnlock(wm->lock);
    if (ret == 0)
        wm->depth--;
    return ret;
}

/* Determines if the current thread owns the lock of a wrapper mutex */
int wrapperMutexOwnLock(struct wrapperMutex *wm) {
    if (wm->lock == NULL) return 1;
    if (wm->lock->skipLock != NULL && wm->lock->skipLock()) return 1;
    return mutexOwnLock(wm->lock);
}