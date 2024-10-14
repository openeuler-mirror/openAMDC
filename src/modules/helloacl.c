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
#include <pthread.h>
#include <unistd.h>

// A simple global user
static RedisModuleUser *global;
static uint64_t global_auth_client_id = 0;

/* HELLOACL.REVOKE 
 * Synchronously revoke access from a user. */
int RevokeCommand_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);

    if (global_auth_client_id) {
        RedisModule_DeauthenticateAndCloseClient(ctx, global_auth_client_id);
        return RedisModule_ReplyWithSimpleString(ctx, "OK");
    } else {
        return RedisModule_ReplyWithError(ctx, "Global user currently not used");    
    }
}

/* HELLOACL.RESET 
 * Synchronously delete and re-create a module user. */
int ResetCommand_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);

    RedisModule_FreeModuleUser(global);
    global = RedisModule_CreateModuleUser("global");
    RedisModule_SetModuleUserACL(global, "allcommands");
    RedisModule_SetModuleUserACL(global, "allkeys");
    RedisModule_SetModuleUserACL(global, "on");

    return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

/* Callback handler for user changes, use this to notify a module of 
 * changes to users authenticated by the module */
void HelloACL_UserChanged(uint64_t client_id, void *privdata) {
    REDISMODULE_NOT_USED(privdata);
    REDISMODULE_NOT_USED(client_id);
    global_auth_client_id = 0;
}

/* HELLOACL.AUTHGLOBAL 
 * Synchronously assigns a module user to the current context. */
int AuthGlobalCommand_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);

    if (global_auth_client_id) {
        return RedisModule_ReplyWithError(ctx, "Global user currently used");    
    }

    RedisModule_AuthenticateClientWithUser(ctx, global, HelloACL_UserChanged, NULL, &global_auth_client_id);

    return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

#define TIMEOUT_TIME 1000

/* Reply callback for auth command HELLOACL.AUTHASYNC */
int HelloACL_Reply(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);
    size_t length;

    RedisModuleString *user_string = RedisModule_GetBlockedClientPrivateData(ctx);
    const char *name = RedisModule_StringPtrLen(user_string, &length);

    if (RedisModule_AuthenticateClientWithACLUser(ctx, name, length, NULL, NULL, NULL) == 
            REDISMODULE_ERR) {
        return RedisModule_ReplyWithError(ctx, "Invalid Username or password");    
    }
    return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

/* Timeout callback for auth command HELLOACL.AUTHASYNC */
int HelloACL_Timeout(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);
    return RedisModule_ReplyWithSimpleString(ctx, "Request timedout");
}

/* Private data frees data for HELLOACL.AUTHASYNC command. */
void HelloACL_FreeData(RedisModuleCtx *ctx, void *privdata) {
    REDISMODULE_NOT_USED(ctx);
    RedisModule_FreeString(NULL, privdata);
}

/* Background authentication can happen here. */
void *HelloACL_ThreadMain(void *args) {
    void **targs = args;
    RedisModuleBlockedClient *bc = targs[0];
    RedisModuleString *user = targs[1];
    RedisModule_Free(targs);

    RedisModule_UnblockClient(bc,user);
    return NULL;
}

/* HELLOACL.AUTHASYNC 
 * Asynchronously assigns an ACL user to the current context. */
int AuthAsyncCommand_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 2) return RedisModule_WrongArity(ctx);

    pthread_t tid;
    RedisModuleBlockedClient *bc = RedisModule_BlockClient(ctx, HelloACL_Reply, HelloACL_Timeout, HelloACL_FreeData, TIMEOUT_TIME);
    

    void **targs = RedisModule_Alloc(sizeof(void*)*2);
    targs[0] = bc;
    targs[1] = RedisModule_CreateStringFromString(NULL, argv[1]);

    if (pthread_create(&tid, NULL, HelloACL_ThreadMain, targs) != 0) {
        RedisModule_AbortBlock(bc);
        return RedisModule_ReplyWithError(ctx, "-ERR Can't start thread");
    }

    return REDISMODULE_OK;
}

/* This function must be present on each Redis module. It is used in order to
 * register the commands into the Redis server. */
int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);

    if (RedisModule_Init(ctx,"helloacl",1,REDISMODULE_APIVER_1)
        == REDISMODULE_ERR) return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"helloacl.reset",
        ResetCommand_RedisCommand,"",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"helloacl.revoke",
        RevokeCommand_RedisCommand,"",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"helloacl.authglobal",
        AuthGlobalCommand_RedisCommand,"no-auth",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"helloacl.authasync",
        AuthAsyncCommand_RedisCommand,"no-auth",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    global = RedisModule_CreateModuleUser("global");
    RedisModule_SetModuleUserACL(global, "allcommands");
    RedisModule_SetModuleUserACL(global, "allkeys");
    RedisModule_SetModuleUserACL(global, "on");

    global_auth_client_id = 0;

    return REDISMODULE_OK;
}
