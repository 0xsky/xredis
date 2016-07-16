


 /** \example demo_multi_cluster.cpp
 * This is an example of how to use the xRedis.
 * <br>connect to multi redis clusters 
 * <br>every redis cluster have multi redis server nodes
 * <br>every redis node have a connection pool
 * <br>More details about this example.
 */

#include "xRedisClient.h"

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

    xRedisClient xRedis;
    xRedis.Init(CACHE_TYPE_MAX);
    xRedis.ConnectRedisCache(RedisList1, 3, CACHE_TYPE_1);
    xRedis.ConnectRedisCache(RedisList2, 5, CACHE_TYPE_2);
        
    const char *key = "test";
    const char *value = "test value";

    RedisDBIdx dbi(&xRedis);
    bool bRet = dbi.CreateDBIndex(key, APHash, CACHE_TYPE_1);
    if (!bRet) {
        return 0;
    }

    bool bRet = xRedis.set(dbi, key, value)) \
    if (bRet){
           printf("success \r\n");
     } else {
        printf("error [%s] \r\n", dbi.GetErrInfo());
     }

    string strValue;
    bRet = xRedis.get(dbi, szKey, strValue);
    if (bRet) {
        printf("%s \r\n", strValue.c_str());
    } else {
        printf("error [%s] \r\n", dbi.GetErrInfo());
    }

    return 0;
}


