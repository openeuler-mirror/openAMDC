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

#include "server.h"
#include "cluster.h"

/* ========================== Clients timeouts ============================= */

/* Check if this blocked client timedout (does nothing if the client is
 * not blocked right now). If so send a reply, unblock it, and return 1.
 * Otherwise 0 is returned and no operation is performed. */
int checkBlockedClientTimeout(client *c, mstime_t now) {
    if (c->flags & CLIENT_BLOCKED &&
        c->bpop.timeout != 0
        && c->bpop.timeout < now)
    {
        /* Handle blocking operation specific timeout. */
        replyToBlockedClientTimedOut(c);
        unblockClient(c);
        return 1;
    } else {
        return 0;
    }
}

/* Check for timeouts. Returns non-zero if the client was terminated.
 * The function gets the current time in milliseconds as argument since
 * it gets called multiple times in a loop, so calling gettimeofday() for
 * each iteration would be costly without any actual gain. */
int clientsCronHandleTimeout(client *c, mstime_t now_ms) {
    time_t now = now_ms/1000;

    if (server.maxidletime &&
        /* This handles the idle clients connection timeout if set. */
        !(c->flags & CLIENT_SLAVE) &&   /* No timeout for slaves and monitors */
        !(c->flags & CLIENT_MASTER) &&  /* No timeout for masters */
        !(c->flags & CLIENT_BLOCKED) && /* No timeout for BLPOP */
        !(c->flags & CLIENT_PUBSUB) &&  /* No timeout for Pub/Sub clients */
        (now - c->lastinteraction > server.maxidletime))
    {
        serverLog(LL_VERBOSE,"Closing idle client");
        freeClient(c);
        return 1;
    } else if (c->flags & CLIENT_BLOCKED) {
        /* Cluster: handle unblock & redirect of clients blocked
         * into keys no longer served by this server. */
        if (server.cluster_enabled) {
            if (clusterRedirectBlockedClientIfNeeded(c))
                unblockClient(c);
        }
    }
    return 0;
}

/* For blocked clients timeouts we populate a radix tree of 128 bit keys
 * composed as such:
 *
 *  [8 byte big endian expire time]+[8 byte client ID]
 *
 * We don't do any cleanup in the Radix tree: when we run the clients that
 * reached the timeout already, if they are no longer existing or no longer
 * blocked with such timeout, we just go forward.
 *
 * Every time a client blocks with a timeout, we add the client in
 * the tree. In beforeSleep() we call handleBlockedClientsTimeout() to run
 * the tree and unblock the clients. */

#define CLIENT_ST_KEYLEN 16    /* 8 bytes mstime + 8 bytes client ID. */

/* Given client ID and timeout, write the resulting radix tree key in buf. */
void encodeTimeoutKey(unsigned char *buf, uint64_t timeout, client *c) {
    timeout = htonu64(timeout);
    memcpy(buf,&timeout,sizeof(timeout));
    memcpy(buf+8,&c,sizeof(c));
    if (sizeof(c) == 4) memset(buf+12,0,4); /* Zero padding for 32bit target. */
}

/* Given a key encoded with encodeTimeoutKey(), resolve the fields and write
 * the timeout into *toptr and the client pointer into *cptr. */
void decodeTimeoutKey(unsigned char *buf, uint64_t *toptr, client **cptr) {
    memcpy(toptr,buf,sizeof(*toptr));
    *toptr = ntohu64(*toptr);
    memcpy(cptr,buf+8,sizeof(*cptr));
}

/* Add the specified client id / timeout as a key in the radix tree we use
 * to handle blocked clients timeouts. The client is not added to the list
 * if its timeout is zero (block forever). */
void addClientToTimeoutTable(client *c) {
    if (c->bpop.timeout == 0) return;
    uint64_t timeout = c->bpop.timeout;
    unsigned char buf[CLIENT_ST_KEYLEN];
    encodeTimeoutKey(buf,timeout,c);
    if (raxTryInsert(server.clients_timeout_table[threadId],buf,sizeof(buf),NULL,NULL))
        c->flags |= CLIENT_IN_TO_TABLE;
}

/* Remove the client from the table when it is unblocked for reasons
 * different than timing out. */
void removeClientFromTimeoutTable(client *c) {
    if (!(c->flags & CLIENT_IN_TO_TABLE)) return;
    c->flags &= ~CLIENT_IN_TO_TABLE;
    uint64_t timeout = c->bpop.timeout;
    unsigned char buf[CLIENT_ST_KEYLEN];
    encodeTimeoutKey(buf,timeout,c);
    raxRemove(server.clients_timeout_table[threadId],buf,sizeof(buf),NULL);
}

/* This function is called in beforeSleep() in order to unblock clients
 * that are waiting in blocking operations with a timeout set. */
void handleBlockedClientsTimeout(void) {
    if (raxSize(server.clients_timeout_table[threadId]) == 0) return;
    uint64_t now = mstime();
    raxIterator ri;
    raxStart(&ri,server.clients_timeout_table[threadId]);
    raxSeek(&ri,"^",NULL,0);

    while(raxNext(&ri)) {
        uint64_t timeout;
        client *c;
        decodeTimeoutKey(ri.key,&timeout,&c);
        if (timeout >= now) break; /* All the timeouts are in the future. */
        WRAPPER_MUTEX_LOCK(cl, &c->lock);
        c->flags &= ~CLIENT_IN_TO_TABLE;
        checkBlockedClientTimeout(c,now);
        raxRemove(server.clients_timeout_table[threadId],ri.key,ri.key_len,NULL);
        raxSeek(&ri,"^",NULL,0);
    }
    raxStop(&ri);
}

/* Get a timeout value from an object and store it into 'timeout'.
 * The final timeout is always stored as milliseconds as a time where the
 * timeout will expire, however the parsing is performed according to
 * the 'unit' that can be seconds or milliseconds.
 *
 * Note that if the timeout is zero (usually from the point of view of
 * commands API this means no timeout) the value stored into 'timeout'
 * is zero. */
int getTimeoutFromObjectOrReply(client *c, robj *object, mstime_t *timeout, int unit) {
    long long tval;
    long double ftval;

    if (unit == UNIT_SECONDS) {
        if (getLongDoubleFromObjectOrReply(c,object,&ftval,
            "timeout is not a float or out of range") != C_OK)
            return C_ERR;
        tval = (long long) (ftval * 1000.0);
    } else {
        if (getLongLongFromObjectOrReply(c,object,&tval,
            "timeout is not an integer or out of range") != C_OK)
            return C_ERR;
    }

    if (tval < 0) {
        addReplyError(c,"timeout is negative");
        return C_ERR;
    }

    if (tval > 0) {
        tval += mstime();
    }
    *timeout = tval;

    return C_OK;
}
