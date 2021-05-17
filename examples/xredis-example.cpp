/*
 * ----------------------------------------------------------------------------
 * Copyright (c) 2013-2021, xSky <guozhw at gmail dot com>
 * All rights reserved.
 * Distributed under GPL license.
 * ----------------------------------------------------------------------------
 */
 
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "xRedisClient.h"

using namespace xrc;

enum {
 CACHE_TYPE_1, 
 CACHE_TYPE_2,
 CACHE_TYPE_MAX,
};


RedisNode RedisList1[3]=
{
    {0,"127.0.0.1", 7000, "", 2, 5, 0},
    {1,"127.0.0.1", 7000, "", 2, 5, 0},
    {2,"127.0.0.1", 7000, "", 2, 5, 0}
};

RedisNode RedisList2[5]=
{
    {0,"127.0.0.1", 7000, "", 2, 5, 0},
    {1,"127.0.0.1", 7000, "", 2, 5, 0},
    {2,"127.0.0.1", 7000, "", 2, 5, 0},
    {3,"127.0.0.1", 7000, "", 2, 5, 0},
    {4,"127.0.0.1", 7000, "", 2, 5, 0},
};

int main(int argc, char **argv) {

    (void)argc;(void)argv;
    
    xRedisClient xRedis;
    xRedis.Init(CACHE_TYPE_MAX);
    xRedis.ConnectRedisCache(RedisList1, sizeof(RedisList1) / sizeof(RedisNode), 3, CACHE_TYPE_1);
    xRedis.ConnectRedisCache(RedisList2, sizeof(RedisList2) / sizeof(RedisNode), 5, CACHE_TYPE_2);
        
    for (int n = 0; n<1000; n++) {
        char szKey[256] = {0};
        sprintf(szKey, "test_%d", n);
        SliceIndex index(&xRedis, CACHE_TYPE_1);
        index.Create(szKey);
        bool bRet = xRedis.set(index, szKey, "hello redis!");
        if (!bRet){
            printf(" %s %s \n", szKey, index.GetErrInfo());
        }
    }

    for (int n = 0; n<1000; n++) {
        char szKey[256] = {0};
        sprintf(szKey, "test_%d", n);
        SliceIndex index(&xRedis, CACHE_TYPE_1);
        index.Create(szKey);
        std::string strValue;
        xRedis.get(index, szKey, strValue);
        printf("%s \r\n", strValue.c_str());
    }
    

    int n = 10;
    while (n--) {
        xRedis.Keepalive();
        usleep(1000*1000*10);
    }

    xRedis.Release();

    return 0;
}


