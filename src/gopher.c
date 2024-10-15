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

/* Emit an item in Gopher directory listing format:
 * <type><descr><TAB><selector><TAB><hostname><TAB><port>
 * If descr or selector are NULL, then the "(NULL)" string is used instead. */
void addReplyGopherItem(client *c, const char *type, const char *descr,
                        const char *selector, const char *hostname, int port)
{
    sds item = sdscatfmt(sdsempty(),"%s%s\t%s\t%s\t%i\r\n",
                         type, descr,
                         selector ? selector : "(NULL)",
                         hostname ? hostname : "(NULL)",
                         port);
    addReplyProto(c,item,sdslen(item));
    sdsfree(item);
}

/* This is called by processInputBuffer() when an inline request is processed
 * with Gopher mode enabled, and the request happens to have zero or just one
 * argument. In such case we get the relevant key and reply using the Gopher
 * protocol. */
void processGopherRequest(client *c) {
    robj *keyname = c->argc == 0 ? createStringObject("/",1) : c->argv[0];
    robj *o = lookupKeyRead(c->db,keyname);

    /* If there is no such key, return with a Gopher error. */
    if (o == NULL || o->type != OBJ_STRING) {
        char *errstr;
        if (o == NULL)
            errstr = "Error: no content at the specified key";
        else
            errstr = "Error: selected key type is invalid "
                     "for Gopher output";
        addReplyGopherItem(c,"i",errstr,NULL,NULL,0);
        addReplyGopherItem(c,"i","openAMDC Gopher server",NULL,NULL,0);
    } else {
        addReply(c,o);
    }

    /* Cleanup, also make sure to emit the final ".CRLF" line. Note that
     * the connection will be closed immediately after this because the client
     * will be flagged with CLIENT_CLOSE_AFTER_REPLY, in accordance with the
     * Gopher protocol. */
    if (c->argc == 0) decrRefCount(keyname);

    /* Note that in theory we should terminate the Gopher request with
     * ".<CR><LF>" (called Lastline in the RFC) like that:
     *
     * addReplyProto(c,".\r\n",3);
     *
     * However after examining the current clients landscape, it's probably
     * going to do more harm than good for several reasons:
     *
     * 1. Clients should not have any issue with missing .<CR><LF> as for
     *    specification, and in the real world indeed certain servers
     *    implementations never used to send the terminator.
     *
     * 2. OpenAMDC does not know if it's serving a text file or a binary file:
     *    at the same time clients will not remove the ".<CR><LF>" bytes at
     *    tne end when downloading a binary file from the server, so adding
     *    the "Lastline" terminator without knowing the content is just
     *    dangerous.
     *
     * 3. The utility gopher2redis.rb that we provide for openAMDC, and any
     *    other similar tool you may use as Gopher authoring system for
     *    openAMDC, can just add the "Lastline" when needed.
     */
}
