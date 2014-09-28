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
#include "xRedisClient.h"
using namespace std;

#define MAX_REDIS_CONN_POOLSIZE     32      // 每个DB最大连接数
#define MAX_REDIS_CACHE_TYPE        32      // 最大支持的CACHE种类数
#define MAX_REDIS_DB_HASHBASE       32      // 最大HASH分库基数

#define GET_CONNECT_ERROR       "GetConnection ERROR"
#define CONNECT_CLOSED_ERROR    "redis connection be closed"

enum {
    REDISDB_UNCONN,
    REDISDB_WORKING,
    REDISDB_DEAD
};

class RedisConn{
public:
    RedisConn(){
        mCtx      = NULL;
        mHost     = NULL;
        mPass     = NULL;
        mPort     = 0;
        mTimeout  = 0;
        mPoolsize = 0;
        mType     = 0;
        mDbindex  = 0;
    }
    ~RedisConn(){

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
                redisFree(mCtx);
                mCtx = NULL;
            } else {

            }
        } else {
            if (0==strlen(mPass)) {
                bRet = true;
            } else {
                redisReply *reply = static_cast<redisReply *>(redisCommand(mCtx,"AUTH %s", mPass));
                if((NULL==reply)||(strcasecmp(reply->str,"OK") != 0)) {
                    bRet = false;
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
};

typedef std::list<RedisConn *> RedisConnList;
typedef std::list<RedisConn *>::iterator RedisConnIter;

class RedisDBSlice{
public:
    RedisDBSlice(){
        mType     = 0;
        mDbindex  = 0;
        mStatus   = 0;
    }
    ~RedisDBSlice(){

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
                return false;
        }

        try {
            for (unsigned int i = 0; i < poolsize; ++i) {
                RedisConn *pRedisconn = new RedisConn;
                if(NULL==pRedisconn) {
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
            return false;
        }
        return !mConnlist.empty();
    }

    RedisConn *GetConn() {
        RedisConn *pRedisConn = NULL;
        XLOCK(mConnlock);
        if (!mConnlist.empty()) {
            pRedisConn = mConnlist.front();
            mConnlist.pop_front();
        } else {
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
};

class RedisCache{
public:
    RedisCache(){
        mCachetype = 0;
        mHashbase  = 0;
        mDBList    = NULL;
    }
    virtual ~RedisCache(){

    }

    bool InitDB(const unsigned int cachetype, const unsigned int hashbase) {
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
        RedisDBSlice *pdbSclice = &mDBList[dbindex];
        if (NULL==pdbSclice) {
            return REDISDB_UNCONN;
        }
        return pdbSclice->GetStatus();
    }

    unsigned int GetHashBase() const {
        return mHashbase;
    }

private:
    RedisDBSlice *mDBList;
    unsigned int  mCachetype;
    unsigned int  mHashbase;
};


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
