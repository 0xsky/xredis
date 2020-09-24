
 /** \example demo_cluster.cpp
 * This is an example of how to use the xRedis.
 * <br>Connect to a redis cluster which contains three redis node
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

/* 配置一个3分片存储xRedis集群：共三个存储主节点 */
RedisNode RedisList1[3]=
{
    {0,"127.0.0.1", 7000, "", 2, 5, 0},
    {1,"127.0.0.1", 7000, "", 2, 5, 0},
    {2,"127.0.0.1", 7000, "", 2, 5, 0}
};

int main(int argc, char **argv) {
    (void)argc;(void)argv;
    
    xRedisClient xRedis;
    xRedis.Init(CACHE_TYPE_MAX);
    xRedis.ConnectRedisCache(RedisList1, sizeof(RedisList1) / sizeof(RedisNode), 3, CACHE_TYPE_1);

    const char *key = "test";
    const char *value = "test value";

    SliceIndex index(&xRedis,CACHE_TYPE_1);
    bool bRet = index.Create(key);
    if (!bRet) {
        return 0;
    }

    bRet = xRedis.set(index, key, value);
    if (bRet){
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


