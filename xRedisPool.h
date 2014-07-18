/*
 * ----------------------------------------------------------------------------
 * Copyright (c) 2013-2014, xSky <guozhw at gmail dot com>
 * All rights reserved.
 * Distributed under GPL license.
 * ----------------------------------------------------------------------------
 */

#ifndef _XREDIS_POOL_H_
#define _XREDIS_POOL_H_

#include <string.h>
#include <string>
#include <list>
#include "xLock.h"
#include "hiredis.h"
using namespace std;

#define MAX_REDIS_CONN_POOLSIZE     32      // 每个DB最大连接数
#define MAX_REDIS_CACHE_TYPE        32      // 最大支持的CACHE种类数
#define MAX_REDIS_DB_HASHBASE       32      // 最大HASH分库基数

enum {
    REDISDB_UNCONN,
    REDISDB_WORKING,
    REDISDB_DEAD
};

typedef struct REDISCONN{
    REDISCONN(){
        mCtx      = NULL;
        mHost     = NULL;
        mPass     = NULL;
        mPort     = 0;
        mTimeout  = 0;
        mPoolsize = 0;
        mType     = 0;
        mDbindex  = 0;
    }
    ~REDISCONN(){

    }

    void Init(const unsigned int  cahcetype, 
        const unsigned int  dbindex,
        const char         *host,
        const unsigned int  port,
        const char         *pass,
        const unsigned int  poolsize,
        const unsigned int  timeout) {
            mType     = cahcetype;
            mDbindex  = dbindex;
            mHost     = host;
            mPort     = port;
            mPass     = pass;
            mPoolsize = poolsize;
            mTimeout  = timeout;
    }

    bool RedisConnect() {
        if (NULL!=mCtx){
            redisFree(mCtx);
            mCtx = NULL;
        }

        bool bRet = false;
        struct timeval timeoutVal;
        timeoutVal.tv_sec  = mTimeout;
        timeoutVal.tv_usec = 0;

        mCtx = redisConnectWithTimeout(mHost, mPort, timeoutVal);
        if (NULL == mCtx || mCtx->err) {
            if (NULL!=mCtx) {
                printf("Connect to Redis: type:%u dbindex:%u %s:%u pass:%s poolsize:%u error:%s \n",
                    mType, mDbindex, mHost, mPort, mPass, mPoolsize, mCtx->errstr);
                redisFree(mCtx);
            } else {
                printf("Connection error: can't allocate redis context \n");
            }
        } else {
            printf("Connect to Redis: type:%u dbindex:%u %s:%u pass:%s poolsize:%u success \n",
                mType, mDbindex, mHost, mPort, mPass, mPoolsize);
            if (0==strlen(mPass)) {
                bRet = true;
            } else {
                redisReply *reply = static_cast<redisReply *>(redisCommand(mCtx,"AUTH %s", mPass));
                if((NULL==reply)||(strcasecmp(reply->str,"OK") != 0)) {
                    bRet = false;
                    printf("auth error: type:%u dbindex:%u %s:%u pass:%s poolsize:%u \n",
                        mType, mDbindex, mHost, mPort, mPass, mPoolsize);
                }
                freeReplyObject(reply);
            }
        }

        return bRet;
    }

    bool Ping() const {
        redisReply *reply = static_cast<redisReply *>(redisCommand(mCtx,"PING"));
        bool bRet = (NULL!=reply);
        freeReplyObject(reply);
        return bRet;
    }

    redisContext  *getCtx() const     { return mCtx; }
    unsigned int   getdbindex() const { return mDbindex; }
    unsigned int   GetType() const   { return  mType; }

private:
    // redis connector context
    redisContext *mCtx;
    const char   *mHost;         // redis host
    unsigned int  mPort;          // redis sever port
    const char   *mPass;         // redis server password
    unsigned int  mTimeout;       // connect timeout second
    unsigned int  mPoolsize;      // connect pool size for each redis DB
    unsigned int  mType;          // redis cache pool type 
    unsigned int  mDbindex;       // redis DB index
}RedisConn;

// 连接链表
typedef std::list<RedisConn *> RedisConnList;
typedef std::list<RedisConn *>::iterator RedisConnIter;

typedef struct _REDIS_DATE_SLICE_{
    _REDIS_DATE_SLICE_(){
        mType     = 0;
        mDbindex  = 0;
        mStatus   = 0;
    }
    ~_REDIS_DATE_SLICE_(){

    }

    void Init(const unsigned int  cahcetype, 
        const unsigned int  dbindex) {
            mType     = cahcetype;
            mDbindex  = dbindex;
    }

    bool ConnectRedisNodes(const unsigned int cahcetype, const unsigned int dbindex,
        const char *host, const unsigned int port, const char *passwd,
        const unsigned int poolsize, const unsigned int timeout)
    {
        if((NULL==host)
            ||(cahcetype>MAX_REDIS_CACHE_TYPE)
            ||(dbindex>MAX_REDIS_DB_HASHBASE)
            ||(poolsize>MAX_REDIS_CONN_POOLSIZE)){
                printf("error argv: cahcetype:%u dbindex:%u host:%s port:%u poolsize:%u timeout:%u \r\n", 
                    cahcetype, dbindex, host, port, poolsize, timeout);
                return false;
        }

        try {
            for (unsigned int i = 0; i < poolsize; ++i) {
                RedisConn *pRedisconn = new RedisConn;
                if(NULL==pRedisconn) {
                    printf("error pRedisconn is null, %s %u %u \r\n", host, port, cahcetype);
                    continue;
                }

                pRedisconn->Init(cahcetype, dbindex, host, port, passwd, poolsize, timeout);
                if (pRedisconn->RedisConnect()) {
                    mConnlist.push_back(pRedisconn);
                    mStatus = REDISDB_WORKING;
                } else {
                    delete pRedisconn;
                }
            }
        } catch( ...) {
            printf("connect error  poolsize=%u \n", poolsize);
            return false;
        }
        printf("\n");
        return !mConnlist.empty();
    }

    RedisConn *GetConn() {
        RedisConn *pRedisConn = NULL;
        XLOCK(mConnlock);
        if (!mConnlist.empty()) {
            pRedisConn = mConnlist.front();
            mConnlist.pop_front();
        } else {
            printf("GetConn  error pthread_id=%u \n", (unsigned int)pthread_self());
            mStatus = REDISDB_DEAD;
        }
        return pRedisConn;
    }

    void FreeConn(RedisConn *redisconn) {
        if (NULL!=redisconn) {
            XLOCK(mConnlock);
            mConnlist.push_back(redisconn);
        }
    }

    void CloseConnPool() {
        XLOCK(mConnlock);
        RedisConnIter iter = mConnlist.begin();
        for (; iter != mConnlist.end(); ++iter){
            printf("close dbindex:%u \r\n", mDbindex);
            redisFree((*iter)->getCtx());
            delete *iter;
        }
        mStatus = REDISDB_DEAD;
    }

    void ConnPoolPing() {
        XLOCK(mConnlock);
        RedisConnIter iter = mConnlist.begin();
        for (; iter != mConnlist.end(); ++iter){
            bool bRet = (*iter)->Ping();
            if (!bRet) {
                (*iter)->RedisConnect();
            } else {
                printf("ping dbindex:%u sucess \n", mDbindex);
            }
        }
    }

    unsigned int GetStatus() const {
        return mStatus;
    }

private:
    RedisConnList mConnlist;
    xLock         mConnlock;
    unsigned int  mType;          // redis cache pool type 
    unsigned int  mDbindex;       // redis DB index
    unsigned int  mStatus;        // redis DB status
}RedisDBSlice;

typedef struct _REDIS_CACHE_{
    _REDIS_CACHE_(){
        mCachetype = 0;
        mHashbase  = 0;
        mDBList    = NULL;
    }
    virtual ~_REDIS_CACHE_(){

    }

    bool InitDB(const unsigned int cachetype, const unsigned int hashbase) {

        printf("cachetype:%u  hashbase:%u \r\n", cachetype, hashbase);

        mCachetype = cachetype;
        mHashbase  = hashbase;
        if(NULL==mDBList) {
            mDBList    = new RedisDBSlice[hashbase];
        }

        return true;
    }

    bool ConnectRedisDB(const unsigned int cahcetype, const unsigned int dbindex,
        const char *host, const unsigned int port, const char *passwd,
        const unsigned int poolsize, const unsigned int timeout) {
            mDBList[dbindex].Init(cahcetype, dbindex);
            return mDBList[dbindex].ConnectRedisNodes(cahcetype, dbindex, host, port,
                passwd, poolsize, timeout);
    }

    RedisConn *GetConn(const unsigned int dbindex) {
        return mDBList[dbindex].GetConn();
    }

    void FreeConn(RedisConn *redisconn) {
        return mDBList[redisconn->getdbindex()].FreeConn(redisconn);
    }

    void ClosePool() {
        for (unsigned int i=0; i<mHashbase; i++) {
            mDBList[i].CloseConnPool();
        }
        delete [] mDBList;
        mDBList = NULL;
    }

    void KeepAlive() {
        for (unsigned int i=0; i<mHashbase; i++) {
            mDBList[i].ConnPoolPing();
        }
    }

    unsigned int GetDBStatus(unsigned int dbindex) {
        return mDBList[dbindex].GetStatus();
    }

    unsigned int GetHashBase() const {
        return mHashbase;
    }

private:
    RedisDBSlice *mDBList;
    unsigned int  mCachetype;
    unsigned int  mHashbase;
}RedisCache;


class RedisPool{
public:
    RedisPool();
    ~RedisPool();

    static RedisPool *GetInstance();

    bool Init(const unsigned int typesize);
    bool setHashBase(const unsigned int cachetype, const unsigned int hashbase);
    unsigned int getHashBase(const unsigned int cachetype);
    bool ConnectRedisDB( unsigned int cachetype,  unsigned int dbindex,
        const char* host,  unsigned int port, const char* passwd,
        unsigned int poolsize,  unsigned int timeout);
    void Release();

    static bool CheckReply(const redisReply* reply);
    static void FreeReply(const redisReply* reply);
    void KeepAlive();

    RedisConn *GetConnection(const unsigned int cachetype, const unsigned int index);
    void FreeConnection(RedisConn* redisconn);

private:
    RedisCache     *mRedisCacheList;
    unsigned int    mTypeSize;
};


#endif
