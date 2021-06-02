
/*
 * ----------------------------------------------------------------------------
 * Copyright (c) 2013-2021, xSky <guozhw at gmail dot com>
 * All rights reserved.
 * Distributed under GPL license.
 * ----------------------------------------------------------------------------
 */

/** \example demo_multi_cluster.cpp
 * This is an example of how to use the xRedis.
 * <br>connect to multi redis clusters
 * <br>every redis cluster have multi redis server nodes
 * <br>every redis node have a connection pool
 * <br>More details about this example.
 */

#include <stdio.h>

#include "xRedisClient.h"

using namespace xrc;

enum {
    CACHE_TYPE_1,
    CACHE_TYPE_2,
    CACHE_TYPE_MAX,
};

/* 配置一个3分片存储xRedis集群 CACHE_TYPE_1：共3个存储主节点 */
RedisNode RedisList1[3] = {
    { .dbindex = 0, .host = "127.0.0.1", .port = 6379, .passwd = "", .poolsize = 4, .timeout = 5, .role = MASTER },
    { .dbindex = 1, .host = "127.0.0.2", .port = 6379, .passwd = "", .poolsize = 4, .timeout = 5, .role = MASTER },
    { .dbindex = 2, .host = "127.0.0.3", .port = 6379, .passwd = "", .poolsize = 4, .timeout = 5, .role = MASTER }
};

/* 配置一个5分片存储xRedis集群 CACHE_TYPE_2：共5个存储主节点 */
RedisNode RedisList2[5] = {
    { .dbindex = 0, .host = "127.0.0.1", .port = 6379, .passwd = "", .poolsize = 4, .timeout = 5, .role = MASTER },
    { .dbindex = 1, .host = "127.0.0.2", .port = 6379, .passwd = "", .poolsize = 4, .timeout = 5, .role = MASTER },
    { .dbindex = 2, .host = "127.0.0.3", .port = 6379, .passwd = "", .poolsize = 4, .timeout = 5, .role = MASTER },
    { .dbindex = 3, .host = "127.0.0.4", .port = 6379, .passwd = "", .poolsize = 4, .timeout = 5, .role = MASTER },
    { .dbindex = 4, .host = "127.0.0.5", .port = 6379, .passwd = "", .poolsize = 4, .timeout = 5, .role = MASTER },
};

int main(int argc, char** argv)
{
    (void)argc;
    (void)argv;

    xRedisClient xRedis;
    xRedis.Init(CACHE_TYPE_MAX);
    xRedis.ConnectRedisCache(RedisList1, sizeof(RedisList1) / sizeof(RedisNode),
        3, CACHE_TYPE_1);
    xRedis.ConnectRedisCache(RedisList2, sizeof(RedisList2) / sizeof(RedisNode),
        5, CACHE_TYPE_2);

    const char* key = "test";
    const char* value = "test value";

    SliceIndex index(&xRedis, CACHE_TYPE_1);
    bool bRet = index.Create(key);
    if (!bRet) {
        return 0;
    }

    bRet = xRedis.set(index, key, value);
    if (bRet) {
        printf("success \r\n");
    } else {
        printf("error [%s] \r\n", index.GetErrInfo());
    }

    std::string strValue;
    bRet = xRedis.get(index, key, strValue);
    if (bRet) {
        printf("%s \r\n", strValue.c_str());
    } else {
        printf("error [%s] \r\n", index.GetErrInfo());
    }

    return 0;
}
