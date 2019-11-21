


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
// AP Hash Function
unsigned int APHash(const char *str) {
    unsigned int hash = 0;
    int i;
    for (i=0; *str; i++) {
        if ((i&  1) == 0) {
            hash ^= ((hash << 7) ^ (*str++) ^ (hash >> 3));
        } else {
            hash ^= (~((hash << 11) ^ (*str++) ^ (hash >> 5)));
        }
    }
    return (hash&  0x7FFFFFFF);
}

enum {
 CACHE_TYPE_1, 
 CACHE_TYPE_2,
 CACHE_TYPE_MAX,
};

/* 有3个redis分片存储节点的集群*/
RedisNode RedisList1[3]=
{
    {0,"10.10.0.1", 7000, "", 2, 5, 0},
    {1,"10.10.0.2", 7000, "", 2, 5, 0},
    {2,"10.10.0.3", 7000, "", 2, 5, 0}
};

/* 有5个redis分片存储节点的集群 */
RedisNode RedisList2[5]=
{
    {0,"10.10.1.1", 7000, "", 2, 5, 0},
    {1,"10.10.1.2", 7000, "", 2, 5, 0},
    {2,"10.10.1.3", 7000, "", 2, 5, 0},
    {3,"10.10.1.4", 7000, "", 2, 5, 0},
    {4,"10.10.1.5", 7000, "", 2, 5, 0},
};

int main(int argc, char **argv) {
    (void)argc;(void)argv;
    
    xRedisClient xRedis;
    xRedis.Init(CACHE_TYPE_MAX);
    xRedis.ConnectRedisCache(RedisList1, sizeof(RedisList1) / sizeof(RedisNode),3, CACHE_TYPE_1);
    xRedis.ConnectRedisCache(RedisList2, sizeof(RedisList2) / sizeof(RedisNode), 5, CACHE_TYPE_2);
        
    const char *key = "test";
    const char *value = "test value";

    RedisDBIdx dbi(&xRedis);
    bool bRet = dbi.CreateDBIndex(key, APHash, CACHE_TYPE_1);
    if (!bRet) {
        return 0;
    }

    bRet = xRedis.set(dbi, key, value);
    if (bRet){
           printf("success \r\n");
     } else {
        printf("error [%s] \r\n", dbi.GetErrInfo());
     }

    std::string strValue;
    bRet = xRedis.get(dbi, key, strValue);
    if (bRet) {
        printf("%s \r\n", strValue.c_str());
    } else {
        printf("error [%s] \r\n", dbi.GetErrInfo());
    }

    return 0;
}


