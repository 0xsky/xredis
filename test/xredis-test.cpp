#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "xRedisClient.h"

xRedisClient xClient;

// AP Hash Function
unsigned int APHash(const char *str) {
    unsigned int hash = 0;
    int i;

    for (i=0; *str; i++)
    {
        if ((i & 1) == 0)
        {
            hash ^= ((hash << 7) ^ (*str++) ^ (hash >> 3));
        }
        else
        {
            hash ^= (~((hash << 11) ^ (*str++) ^ (hash >> 5)));
        }
    }

    return (hash & 0x7FFFFFFF);
}

#define CACHE_TYPE_1 1


void test_set() {
    char szKey[256] = {0};
    strcpy(szKey, "test");
    RedisDBIdx dbi(&xClient);
    bool bRet = dbi.CreateDBIndex(szKey, APHash, CACHE_TYPE_1);
    if (bRet) {
        if (xClient.set(dbi, szKey, "testwwwwww")) {
            printf("%s success \r\n", __PRETTY_FUNCTION__);
        } else {
            printf("%s error [%s] \r\n", __PRETTY_FUNCTION__, dbi.GetErrInfo());
        }
    }
}

void test_get() {

    char szKey[256] = {0};
    {
        strcpy(szKey, "test");
        RedisDBIdx dbi(&xClient);
        bool bRet = dbi.CreateDBIndex(szKey, APHash, CACHE_TYPE_1);
        if (bRet) {
            string strData;
            if (xClient.get(dbi, szKey, strData)) {
                printf("%s success data:%s \r\n", __PRETTY_FUNCTION__, strData.c_str());
            } else {
                printf("%s error [%s] \r\n", __PRETTY_FUNCTION__, dbi.GetErrInfo());
            }
        }
    }


    {
        sprintf(szKey, "test_%u", (unsigned int)time(NULL));
        RedisDBIdx dbi(&xClient);
        bool bRet = dbi.CreateDBIndex(szKey, APHash, CACHE_TYPE_1);
        if (bRet) {
            string strData;
            if (xClient.get(dbi, szKey, strData)) {
                printf("%s error data:%s \r\n", __PRETTY_FUNCTION__, strData.c_str());
            } else {
                printf("%s success [%s] \r\n", __PRETTY_FUNCTION__, dbi.GetErrInfo());
            }
        }
    }
}

void test_exists() {
    char szKey[256] = {0};
    strcpy(szKey, "test");
    RedisDBIdx dbi(&xClient);
    bool bRet = dbi.CreateDBIndex(szKey, APHash, CACHE_TYPE_1);
    if (bRet) {
        if (xClient.exists(dbi, szKey)) {
            printf("%s success \r\n", __PRETTY_FUNCTION__);
        } else {
            printf("%s error [%s] \r\n", __PRETTY_FUNCTION__, dbi.GetErrInfo());
        }
    }
}

void test_del() {
    char szKey[256] = {0};
    strcpy(szKey, "test");
    RedisDBIdx dbi(&xClient);
    bool bRet = dbi.CreateDBIndex(szKey, APHash, CACHE_TYPE_1);
    if (bRet) {
        if (xClient.del(dbi, szKey)) {
            printf("%s success \r\n", __PRETTY_FUNCTION__);
        } else {
            printf("%s error [%s] \r\n", __PRETTY_FUNCTION__, dbi.GetErrInfo());
        }
    }
}

void test_mset() {
    char szKey[256] = {0};
    DBIArray vdbi;
    VDATA    kvData;

    for (int i=0; i<10; i++)
    {
        RedisDBIdx dbi(&xClient);
        sprintf(szKey, "mset_key_%d", i);
        dbi.CreateDBIndex(szKey, APHash, CACHE_TYPE_1);
        vdbi.push_back(dbi);
        kvData.push_back(szKey);
        kvData.push_back(szKey);
    }

    if (xClient.mset(vdbi, kvData)) {
        printf("%s success \r\n", __PRETTY_FUNCTION__);
    } else {
        printf("%s error [%s] \r\n", __PRETTY_FUNCTION__, "mset error");
    }
}

void test_mget() {
    char szKey[256] = {0};
    DBIArray vdbi;
    KEYS     kData;
    ReplyData Reply;
    for (int i=0; i<15; i++)
    {
        RedisDBIdx dbi(&xClient);
        sprintf(szKey, "mset_key_%d", i);
        dbi.CreateDBIndex(szKey, APHash, CACHE_TYPE_1);
        vdbi.push_back(dbi);
        kData.push_back(szKey);
    }

    if (xClient.mget(vdbi, kData, Reply)) {
        printf("%s success %d \r\n", __PRETTY_FUNCTION__, Reply.size());
        ReplyData::iterator iter=Reply.begin();
        for (; iter!=Reply.end(); iter++) {
            printf("%d\t%s\r\n", (*iter).type, (*iter).str.c_str());
        }
    } else {
        printf("%s error [%s] \r\n", __PRETTY_FUNCTION__, "mset error");
    }
}

void test_hset() {
    char szHKey[256] = {0};
    strcpy(szHKey, "hashtest");
    RedisDBIdx dbi(&xClient);
    bool bRet = dbi.CreateDBIndex(szHKey, APHash, CACHE_TYPE_1);
    if (bRet) {
        int64_t count = 0;
        if (xClient.hset(dbi, szHKey, "filed1", "filed1_values", count)) {
            printf("%s success \r\n", __PRETTY_FUNCTION__);
        } else {
            printf("%s error [%s] \r\n", __PRETTY_FUNCTION__, dbi.GetErrInfo());
        }
    }
}


int main(int argc, char **argv) {

    xClient.Init();

    RedisNode RedisList1[3]=
    {
        {0,"127.0.0.1", 7000, "", 2, 5},
        {1,"127.0.0.1", 7000, "", 2, 5},
        {2,"127.0.0.1", 7000, "", 2, 5}
    };

    xClient.ConnectRedisCache(RedisList1, 3, CACHE_TYPE_1);
        
    test_set();
    test_get();
    test_exists();
    test_del();
    test_hset();
    test_mset();
    test_mget();


    //int n = 10;
    //while (n--) {
    //    xClient.KeepAlive();
    //    usleep(1000*1000*10);
    //}

    xClient.release();

    return 0;
}


