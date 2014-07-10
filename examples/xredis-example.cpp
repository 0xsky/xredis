#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "xRedisClient.h"

// AP Hash Function
unsigned int APHash(const char *str) {
    unsigned int hash = 0;
    int i;

    for (i=0; *str; i++)
    {
        if ((i&  1) == 0)
        {
            hash ^= ((hash << 7) ^ (*str++) ^ (hash >> 3));
        }
        else
        {
            hash ^= (~((hash << 11) ^ (*str++) ^ (hash >> 5)));
        }
    }

    return (hash&  0x7FFFFFFF);
}

#define CACHE_TYPE_1 1
#define CACHE_TYPE_2 2


RedisNode RedisList1[3]=
{
    {0,"127.0.0.1", 7000, "", 2, 5},
    {1,"127.0.0.1", 7000, "", 2, 5},
    {2,"127.0.0.1", 7000, "", 2, 5}
};

RedisNode RedisList2[5]=
{
    {0,"127.0.0.1", 7000, "", 2, 5},
    {1,"127.0.0.1", 7000, "", 2, 5},
    {2,"127.0.0.1", 7000, "", 2, 5},
    {3,"127.0.0.1", 7000, "", 2, 5},
    {4,"127.0.0.1", 7000, "", 2, 5},
};

int main(int argc, char **argv) {

    xRedisClient xRedis;
    xRedis.Init();
    xRedis.ConnectRedisCache(RedisList1, 3, CACHE_TYPE_1);
    xRedis.ConnectRedisCache(RedisList2, 5, CACHE_TYPE_2);
        
    for (int n = 0; n<1000; n++) {
        char szKey[256] = {0};
        sprintf(szKey, "test_%d", n);
        RedisDBIdx dbi;
        dbi = xRedis.GetDBIndex(szKey, APHash, CACHE_TYPE_1);
        bool bRet = xRedis.set(dbi, szKey, "hello redis!");
        if (!bRet){
            printf(" %s %s \n", szKey, dbi.GetErrInfo());
        }
    }

    for (int n = 0; n<1000; n++) {
        char szKey[256] = {0};
        sprintf(szKey, "test_%d", n);
        RedisDBIdx dbi;
        dbi = xRedis.GetDBIndex(szKey, APHash, CACHE_TYPE_1);
        string strValue;
        xRedis.get(dbi, szKey, strValue);
        printf("%s \r\n", strValue.c_str());
    }
    
    int n = 10;
    while (n--) {
        xRedis.KeepAlive();
        usleep(1000*1000*10);
    }

    xRedis.release();

    return 0;
}


