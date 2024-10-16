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

#define REDISMODULE_EXPERIMENTAL_API
#include "../redismodule.h"
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>

#define MSGTYPE_PING 1
#define MSGTYPE_PONG 2

/* HELLOCLUSTER.PINGALL */
int PingallCommand_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);

    RedisModule_SendClusterMessage(ctx,NULL,MSGTYPE_PING,(unsigned char*)"Hey",3);
    return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

/* HELLOCLUSTER.LIST */
int ListCommand_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);

    size_t numnodes;
    char **ids = RedisModule_GetClusterNodesList(ctx,&numnodes);
    if (ids == NULL) {
        return RedisModule_ReplyWithError(ctx,"Cluster not enabled");
    }

    RedisModule_ReplyWithArray(ctx,numnodes);
    for (size_t j = 0; j < numnodes; j++) {
        int port;
        RedisModule_GetClusterNodeInfo(ctx,ids[j],NULL,NULL,&port,NULL);
        RedisModule_ReplyWithArray(ctx,2);
        RedisModule_ReplyWithStringBuffer(ctx,ids[j],REDISMODULE_NODE_ID_LEN);
        RedisModule_ReplyWithLongLong(ctx,port);
    }
    RedisModule_FreeClusterNodesList(ids);
    return REDISMODULE_OK;
}

/* Callback for message MSGTYPE_PING */
void PingReceiver(RedisModuleCtx *ctx, const char *sender_id, uint8_t type, const unsigned char *payload, uint32_t len) {
    RedisModule_Log(ctx,"notice","PING (type %d) RECEIVED from %.*s: '%.*s'",
        type,REDISMODULE_NODE_ID_LEN,sender_id,(int)len, payload);
    RedisModule_SendClusterMessage(ctx,NULL,MSGTYPE_PONG,(unsigned char*)"Ohi!",4);
    RedisModule_Call(ctx, "INCR", "c", "pings_received");
}

/* Callback for message MSGTYPE_PONG. */
void PongReceiver(RedisModuleCtx *ctx, const char *sender_id, uint8_t type, const unsigned char *payload, uint32_t len) {
    RedisModule_Log(ctx,"notice","PONG (type %d) RECEIVED from %.*s: '%.*s'",
        type,REDISMODULE_NODE_ID_LEN,sender_id,(int)len, payload);
}

/* This function must be present on each Redis module. It is used in order to
 * register the commands into the Redis server. */
int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);

    if (RedisModule_Init(ctx,"hellocluster",1,REDISMODULE_APIVER_1)
        == REDISMODULE_ERR) return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"hellocluster.pingall",
        PingallCommand_RedisCommand,"readonly",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"hellocluster.list",
        ListCommand_RedisCommand,"readonly",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    /* Disable Redis Cluster sharding and redirections. This way every node
     * will be able to access every possible key, regardless of the hash slot.
     * This way the PING message handler will be able to increment a specific
     * variable. Normally you do that in order for the distributed system
     * you create as a module to have total freedom in the keyspace
     * manipulation. */
    RedisModule_SetClusterFlags(ctx,REDISMODULE_CLUSTER_FLAG_NO_REDIRECTION);

    /* Register our handlers for different message types. */
    RedisModule_RegisterClusterMessageReceiver(ctx,MSGTYPE_PING,PingReceiver);
    RedisModule_RegisterClusterMessageReceiver(ctx,MSGTYPE_PONG,PongReceiver);
    return REDISMODULE_OK;
}
