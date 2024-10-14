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

#ifndef __CLICOMMON_H
#define __CLICOMMON_H

#include <hiredis.h>

typedef struct cliSSLconfig {
    /* Requested SNI, or NULL */
    char *sni;
    /* CA Certificate file, or NULL */
    char *cacert;
    /* Directory where trusted CA certificates are stored, or NULL */
    char *cacertdir;
    /* Skip server certificate verification. */
    int skip_cert_verify;
    /* Client certificate to authenticate with, or NULL */
    char *cert;
    /* Private key file to authenticate with, or NULL */
    char *key;
    /* Prefered cipher list, or NULL (applies only to <= TLSv1.2) */
    char* ciphers;
    /* Prefered ciphersuites list, or NULL (applies only to TLSv1.3) */
    char* ciphersuites;
} cliSSLconfig;

/* Wrapper around redisSecureConnection to avoid hiredis_ssl dependencies if
 * not building with TLS support.
 */
int cliSecureConnection(redisContext *c, cliSSLconfig config, const char **err);

/* Wrapper around hiredis to allow arbitrary reads and writes.
 *
 * We piggybacks on top of hiredis to achieve transparent TLS support,
 * and use its internal buffers so it can co-exist with commands
 * previously/later issued on the connection.
 *
 * Interface is close to enough to read()/write() so things should mostly
 * work transparently.
 */

/* Write a raw buffer through a redisContext. If we already have something
 * in the buffer (leftovers from hiredis operations) it will be written
 * as well.
 */
ssize_t cliWriteConn(redisContext *c, const char *buf, size_t buf_len);

/* Wrapper around OpenSSL (libssl and libcrypto) initialisation.
 */
int cliSecureInit();

#endif /* __CLICOMMON_H */
