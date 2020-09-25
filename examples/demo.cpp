

/** \example demo.cpp
 * This is an example of how to use the xRedis.
 * <br>This demo connect to single redis server with connection pool
 * <br>More details about this example.
 */

#include <cstdio>
#include "xRedisClient.h"

using namespace xrc;

enum {
 CACHE_TYPE_1, 
 CACHE_TYPE_2,
 CACHE_TYPE_MAX,
};

/* 配置单个redis存储节点 */
RedisNode RedisList1[1]=
{
    {0,"127.0.0.1", 7000, "", 8, 5, 0}
};


int main(int argc, char **argv) {
    (void)argc;(void)argv;
    
    xRedisClient xRedis;
    xRedis.Init(CACHE_TYPE_MAX);
    xRedis.ConnectRedisCache(RedisList1, sizeof(RedisList1) / sizeof(RedisNode), 1, CACHE_TYPE_1);
        
    const char *key = "test";
    const char *value = "test value";
    SliceIndex index(&xRedis, CACHE_TYPE_1);
    index.Create(key);

    bool bRet = xRedis.set(index, key, value); 
    if(bRet){
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


