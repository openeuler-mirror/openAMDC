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

#ifndef __REDIS_RIO_H
#define __REDIS_RIO_H

#include <stdio.h>
#include <stdint.h>
#include "sds.h"
#include "connection.h"

#define RIO_FLAG_READ_ERROR (1<<0)
#define RIO_FLAG_WRITE_ERROR (1<<1)

struct _rio {
    /* Backend functions.
     * Since this functions do not tolerate short writes or reads the return
     * value is simplified to: zero on error, non zero on complete success. */
    size_t (*read)(struct _rio *, void *buf, size_t len);
    size_t (*write)(struct _rio *, const void *buf, size_t len);
    off_t (*tell)(struct _rio *);
    int (*flush)(struct _rio *);
    /* The update_cksum method if not NULL is used to compute the checksum of
     * all the data that was read or written so far. The method should be
     * designed so that can be called with the current checksum, and the buf
     * and len fields pointing to the new block of data to add to the checksum
     * computation. */
    void (*update_cksum)(struct _rio *, const void *buf, size_t len);

    /* The current checksum and flags (see RIO_FLAG_*) */
    uint64_t cksum, flags;

    /* number of bytes read or written */
    size_t processed_bytes;

    /* maximum single read or write chunk size */
    size_t max_processing_chunk;

    /* Backend-specific vars. */
    union {
        /* In-memory buffer target. */
        struct {
            sds ptr;
            off_t pos;
        } buffer;
        /* In-memory cbuffer target. */
        struct {
            char* ptr;
            size_t len;
            off_t pos;
        } cbuffer;
        /* Stdio file pointer target. */
        struct {
            FILE *fp;
            off_t buffered; /* Bytes written since last fsync. */
            off_t autosync; /* fsync after 'autosync' bytes written. */
        } file;
        /* Connection object (used to read from socket) */
        struct {
            connection *conn;   /* Connection */
            off_t pos;    /* pos in buf that was returned */
            sds buf;      /* buffered data */
            size_t read_limit;  /* don't allow to buffer/read more than that */
            size_t read_so_far; /* amount of data read from the rio (not buffered) */
        } conn;
        /* FD target (used to write to pipe). */
        struct {
            int fd;       /* File descriptor. */
            off_t pos;
            sds buf;
        } fd;
    } io;
};

typedef struct _rio rio;

/* The following functions are our interface with the stream. They'll call the
 * actual implementation of read / write / tell, and will update the checksum
 * if needed. */

static inline size_t rioWrite(rio *r, const void *buf, size_t len) {
    if (r->flags & RIO_FLAG_WRITE_ERROR) return 0;
    while (len) {
        size_t bytes_to_write = (r->max_processing_chunk && r->max_processing_chunk < len) ? r->max_processing_chunk : len;
        if (r->update_cksum) r->update_cksum(r,buf,bytes_to_write);
        if (r->write(r,buf,bytes_to_write) == 0) {
            r->flags |= RIO_FLAG_WRITE_ERROR;
            return 0;
        }
        buf = (char*)buf + bytes_to_write;
        len -= bytes_to_write;
        r->processed_bytes += bytes_to_write;
    }
    return 1;
}

static inline size_t rioRead(rio *r, void *buf, size_t len) {
    if (r->flags & RIO_FLAG_READ_ERROR) return 0;
    while (len) {
        size_t bytes_to_read = (r->max_processing_chunk && r->max_processing_chunk < len) ? r->max_processing_chunk : len;
        if (r->read(r,buf,bytes_to_read) == 0) {
            r->flags |= RIO_FLAG_READ_ERROR;
            return 0;
        }
        if (r->update_cksum) r->update_cksum(r,buf,bytes_to_read);
        buf = (char*)buf + bytes_to_read;
        len -= bytes_to_read;
        r->processed_bytes += bytes_to_read;
    }
    return 1;
}

static inline off_t rioTell(rio *r) {
    return r->tell(r);
}

static inline int rioFlush(rio *r) {
    return r->flush(r);
}

/* This function allows to know if there was a read error in any past
 * operation, since the rio stream was created or since the last call
 * to rioClearError(). */
static inline int rioGetReadError(rio *r) {
    return (r->flags & RIO_FLAG_READ_ERROR) != 0;
}

/* Like rioGetReadError() but for write errors. */
static inline int rioGetWriteError(rio *r) {
    return (r->flags & RIO_FLAG_WRITE_ERROR) != 0;
}

static inline void rioClearErrors(rio *r) {
    r->flags &= ~(RIO_FLAG_READ_ERROR|RIO_FLAG_WRITE_ERROR);
}

void rioInitWithFile(rio *r, FILE *fp);
void rioInitWithBuffer(rio *r, sds s);
void rioInitWithCBuffer(rio *r, char *ptr, size_t len);
void rioInitWithConn(rio *r, connection *conn, size_t read_limit);
void rioInitWithFd(rio *r, int fd);

void rioFreeFd(rio *r);
void rioFreeConn(rio *r, sds* out_remainingBufferedData);

size_t rioWriteBulkCount(rio *r, char prefix, long count);
size_t rioWriteBulkString(rio *r, const char *buf, size_t len);
size_t rioWriteBulkLongLong(rio *r, long long l);
size_t rioWriteBulkDouble(rio *r, double d);

struct redisObject;
int rioWriteBulkObject(rio *r, struct redisObject *obj);
int rewriteListObject(rio *r, struct redisObject *key, struct redisObject *o);
int rewriteSetObject(rio *r, struct redisObject *key, struct redisObject *o);
int rewriteSortedSetObject(rio *r, struct redisObject *key, struct redisObject *o);
int rewriteHashObject(rio *r, struct redisObject *key, struct redisObject *o);
int rewriteStreamObject(rio *r, struct redisObject *key, struct redisObject *o);
int rewriteModuleObject(rio *r, struct redisObject *key, struct redisObject *o);

void rioGenericUpdateChecksum(rio *r, const void *buf, size_t len);
void rioSetAutoSync(rio *r, off_t bytes);

#endif
