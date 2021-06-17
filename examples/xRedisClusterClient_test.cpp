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

#include "xRedisClusterClient.h"

using namespace xrc;

int main(int argc, char** argv)
{
    (void)argc;
    (void)argv;

    xRedisClusterClient redisClusterClient;
    std::string pass;
    bool bRet = redisClusterClient.connect("127.0.0.1", 7001, pass, 4);
    //bool bRet = redisClusterClient.ConnectRedis(argv[1], atoi(argv[2]), argv[3], 4);
    if (!bRet) {
        return -1;
    }

    // VSTRING  redis_arg;
    // redis_arg.push_back("hgetall");
    // redis_arg.push_back("htest");

    RedisResult result;
    redisClusterClient.command(result, "hgetall %s", "htest");

    printf("type:%d integer:%lld str:%s \r\n", result.type(), result.integer(),
        result.str());

    for (size_t i = 0; i < result.elements(); ++i) {
        RedisResult::RedisReply reply = result.element(i);
        printf("type:%d integer:%lld str:%s \r\n", reply.type(), reply.integer(),
            reply.str());
    }

    while (true) {
        usleep(1000 * 1000 * 6);
        redisClusterClient.keepalive();
    }

    return 0;
}
