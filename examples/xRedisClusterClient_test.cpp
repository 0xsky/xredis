#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "xRedisClusterClient.h"

int main(int argc, char **argv) {

    xRedisClusterClient redisclient;
    //bool bRet = redisclient.ConnectRedis("127.0.0.1", 7000, 4);
    bool bRet = redisclient.ConnectRedis(argv[1], atoi(argv[2]), 4);
    if(!bRet) {
        return -1;
    }

    RedisResult result;
    redisclient.RedisCommand(result, "hgetall %s", "htest");
    
    printf("type:%d integer:%lld str:%s \r\n",
        result.type(), result.integer(), result.str());
    
    for(size_t i=0; i< result.elements();++i) {
        RedisResult::RedisReply reply = result.element(i);
        printf("type:%d integer:%lld str:%s \r\n",
            reply.type(), reply.integer(), reply.str());
    }

    
    return 0;
}












