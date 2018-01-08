#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "xRedisClient.h"

enum {
 CACHE_TYPE_1, 
 CACHE_TYPE_2,
 CACHE_TYPE_MAX,
};

int main(int argc, char **argv) {

    (void)argc;(void)argv;
    
    xRedisClient xRedis;
    xRedis.Init(CACHE_TYPE_MAX);
    RedisDBICluster dbi;
    dbi.setType(CACHE_TYPE_1);
    dbi.ConnectRedisCluster("127.0.0.1", 7000, 8);

    RedisNode *redisnodelist = dbi.pRedisNodeList;

    for (int i = 0; i < dbi.uCount;++i)
    {
        printf(" host:%s port:%d \n", redisnodelist[i].host, redisnodelist[i].port);
    }

    bool bRet = xRedis.ConnectRedisCache(redisnodelist, dbi.uCount, 32, CACHE_TYPE_1);
    if (!bRet)
    {
        printf(" ConnectRedisCache error \n");
        return 0;
    }

    for (int n = 0; n<1000; n++) {
        char szKey[256] = {0};
        sprintf(szKey, "test_%d", n);
        
        dbi.ClusterDBI(szKey);
        bRet = xRedis.set(dbi, szKey, "hello redis!");
        if (!bRet){
            printf(" %s %s \n", szKey, dbi.GetErrInfo());
        }
    }

    

    int n = 10;
    while (n--) {
        xRedis.Keepalive();
        usleep(1000*1000*10);
    }

    xRedis.Release();

    return 0;
}


