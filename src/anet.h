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

#ifndef ANET_H
#define ANET_H

#include <sys/types.h>

#define ANET_OK 0
#define ANET_ERR -1
#define ANET_ERR_LEN 256

/* Flags used with certain functions. */
#define ANET_NONE 0
#define ANET_IP_ONLY (1<<0)

#if defined(__sun) || defined(_AIX)
#define AF_LOCAL AF_UNIX
#endif

#ifdef _AIX
#undef ip_len
#endif

/* FD to address string conversion types */
#define FD_TO_PEER_NAME 0
#define FD_TO_SOCK_NAME 1

int anetTcpNonBlockConnect(char *err, const char *addr, int port);
int anetTcpNonBlockBestEffortBindConnect(char *err, const char *addr, int port, const char *source_addr);
int anetResolve(char *err, char *host, char *ipbuf, size_t ipbuf_len, int flags);
int anetTcpServer(char *err, int port, char *bindaddr, int backlog, int reusePort);
int anetTcp6Server(char *err, int port, char *bindaddr, int backlog, int reusePort);
int anetUnixServer(char *err, char *path, mode_t perm, int backlog);
int anetTcpAccept(char *err, int serversock, char *ip, size_t ip_len, int *port);
int anetUnixAccept(char *err, int serversock);
int anetNonBlock(char *err, int fd);
int anetBlock(char *err, int fd);
int anetCloexec(int fd);
int anetEnableTcpNoDelay(char *err, int fd);
int anetDisableTcpNoDelay(char *err, int fd);
int anetSendTimeout(char *err, int fd, long long ms);
int anetRecvTimeout(char *err, int fd, long long ms);
int anetFdToString(int fd, char *ip, size_t ip_len, int *port, int fd_to_str_type);
int anetKeepAlive(char *err, int fd, int interval);
int anetIncomingCpu(char *err, int fd, int cpu);
int anetFormatAddr(char *fmt, size_t fmt_len, char *ip, int port);
int anetFormatFdAddr(int fd, char *buf, size_t buf_len, int fd_to_str_type);

#endif
