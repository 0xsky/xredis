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

#define MAX_REDIS_CONN_POOLSIZE     128      // 每个DB最大连接数
#define MAX_REDIS_CACHE_TYPE        128      // 最大支持的CACHE种类数
#define MAX_REDIS_DB_HASHBASE       128      // 最大HASH分库基数

#define GET_CONNECT_ERROR       "get connection error"
#define CONNECT_CLOSED_ERROR    "redis connection be closed"


enum {
    REDISDB_UNCONN,
    REDISDB_WORKING,
    REDISDB_DEAD
};

class RedisConn
{
public:
    RedisConn()
    {
        mCtx      = NULL;
        mPort     = 0;
        mTimeout  = 0;
        mPoolsize = 0;
        mType     = 0;
        mDbindex  = 0;
    }
    ~RedisConn()
    {

    }

    void Init(unsigned int cahcetype,
              unsigned int dbindex,
              const std::string&  host,
              unsigned int  port,
              const std::string&  pass,
              unsigned int poolsize,
              unsigned int timeout,
              unsigned int role,
              unsigned int slaveidx
             )
    {
        mType     = cahcetype;
        mDbindex  = dbindex;
        mHost     = host;
        mPort     = port;
        mPass     = pass;
        mPoolsize = poolsize;
        mTimeout  = timeout;
        mRole     = role;
        mSlaveIdx = slaveidx;
    }

    bool RedisConnect()
    {
        if (NULL != mCtx) {
            redisFree(mCtx);
            mCtx = NULL;
        }

        bool bRet = false;
        struct timeval timeoutVal;
        timeoutVal.tv_sec  = mTimeout;
        timeoutVal.tv_usec = 0;

        mCtx = redisConnectWithTimeout(mHost.c_str(), mPort, timeoutVal);
        if (NULL == mCtx || mCtx->err) {
            if (NULL!=mCtx) {
                redisFree(mCtx);
                mCtx = NULL;
            } else {

            }
        } else {
            if (0==mPass.length()) {
                bRet = true;
            } else {
                redisReply *reply = static_cast<redisReply *>(redisCommand(mCtx,"AUTH %s", mPass.c_str()));
                if((NULL==reply)||(strcasecmp(reply->str,"OK") != 0)) {
                    bRet = false;
                }
                bRet = true;
                freeReplyObject(reply);
            }
        }

        return bRet;
    }

    bool Ping() const
    {
        redisReply *reply = static_cast<redisReply *>(redisCommand(mCtx, "PING"));
        bool bRet = (NULL != reply);
        freeReplyObject(reply);
        return bRet;
    }

    redisContext  *getCtx() const     { return mCtx; }
    unsigned int   getdbindex() const { return mDbindex; }
    unsigned int   GetType() const   { return  mType; }
    unsigned int   GetRole() const   { return  mRole; }
    unsigned int   GetSlaveIdx() const   { return  mSlaveIdx; }

private:
    // redis connector context
    redisContext *mCtx;
    string        mHost;         // redis host
    unsigned int  mPort;          // redis sever port
    string        mPass;         // redis server password
    unsigned int  mTimeout;       // connect timeout second
    unsigned int  mPoolsize;      // connect pool size for each redis DB
    unsigned int  mType;          // redis cache pool type
    unsigned int  mDbindex;       // redis DB index
    unsigned int  mRole;          // redis role
    unsigned int  mSlaveIdx;      // the index in the slave group
};

typedef std::list<RedisConn*> RedisConnPool;
typedef std::list<RedisConn*>::iterator RedisConnIter;

typedef std::vector<RedisConnPool*> RedisSlaveGroup;
typedef std::vector<RedisConnPool*>::iterator RedisSlaveGroupIter;

typedef struct _RedisDBSlice_Conn_ {
    RedisConnPool   RedisMasterConn;
    RedisSlaveGroup RedisSlaveConn;
    xLock           Masterlock;
    xLock           Slavelock;
} RedisSliceConn;


class RedisDBSlice
{
public:
    RedisDBSlice()
    {
        mType     = 0;
        mDbindex  = 0;
        mStatus   = 0;
        mHaveSlave = false;
    }
    ~RedisDBSlice()
    {

    }

    void Init(const unsigned int  cahcetype,
              const unsigned int  dbindex)
    {
        mType     = cahcetype;
        mDbindex  = dbindex;
    }

    bool ConnectRedisNodes(unsigned int cahcetype, unsigned int dbindex,
                           const std::string& host,  unsigned int port, const std::string& passwd,
                           unsigned int poolsize,  unsigned int timeout, int role)
    {
        bool bRet = false;
        if ((host.empty())
                || (cahcetype > MAX_REDIS_CACHE_TYPE)
                || (dbindex > MAX_REDIS_DB_HASHBASE)
                || (poolsize > MAX_REDIS_CONN_POOLSIZE)) {
            printf("error argv: cahcetype:%u dbindex:%u host:%s port:%u poolsize:%u timeout:%u \r\n",
                   cahcetype, dbindex, host.c_str(), port, poolsize, timeout);
            return false;
        }

        try {
            if (MASTER == role) {
                XLOCK(mSliceConn.Masterlock);
                for (unsigned int i = 0; i < poolsize; ++i) {
                    RedisConn *pRedisconn = new RedisConn;
                    if (NULL == pRedisconn) {
                        printf("error pRedisconn is null, %s %u %u \r\n", host.c_str(), port, cahcetype);
                        continue;
                    }

                    pRedisconn->Init(cahcetype, dbindex, host.c_str(), port, passwd.c_str(), poolsize, timeout, role, 0);
                    if (pRedisconn->RedisConnect()) {
                        mSliceConn.RedisMasterConn.push_back(pRedisconn);
                        mStatus = REDISDB_WORKING;
                    } else {
                        delete pRedisconn;
                    }
                }
                bRet = true;
            }else if (SLAVE == role) {
                XLOCK(mSliceConn.Slavelock);
                RedisConnPool *pSlaveNode = new RedisConnPool;
                int slave_idx = mSliceConn.RedisSlaveConn.size();
                for (unsigned int i = 0; i < poolsize; ++i) {
                    RedisConn *pRedisconn = new RedisConn;
                    if (NULL == pRedisconn) {
                        
                        continue;
                    }

                    pRedisconn->Init(cahcetype, dbindex, host.c_str(), port, passwd.c_str(), poolsize, timeout, role, slave_idx);
                    if (pRedisconn->RedisConnect()) {
                        pSlaveNode->push_back(pRedisconn);
                    } else {
                        delete pRedisconn;
                    }
                }
                mSliceConn.RedisSlaveConn.push_back(pSlaveNode);
                bRet = true;
                mHaveSlave = true;
            } else {
                bRet = false;
            }
        } catch( ...) {
            return false;
        }
        return bRet;
    }

    RedisConn *GetMasterConn()
    {
        RedisConn *pRedisConn = NULL;
        XLOCK(mSliceConn.Masterlock);
        if (!mSliceConn.RedisMasterConn.empty()) {
            pRedisConn = mSliceConn.RedisMasterConn.front();
            mSliceConn.RedisMasterConn.pop_front();
        } else {
            mStatus = REDISDB_DEAD;
        }
        return pRedisConn;
    }

    RedisConn *GetSlaveConn()
    {
        RedisConn *pRedisConn = NULL;
        XLOCK(mSliceConn.Slavelock);
        if (!mSliceConn.RedisSlaveConn.empty()) {
            size_t slave_cnt = mSliceConn.RedisSlaveConn.size();
            unsigned int idx = rand() % slave_cnt;
            RedisConnPool *pSlave = mSliceConn.RedisSlaveConn[idx];
            pRedisConn = pSlave->front();
            pSlave->pop_front();
            if (idx != pRedisConn->GetSlaveIdx()) {
                printf("mSlaveIdx pthread_id=%u \n", (unsigned int)pthread_self());
            }
        } 
        return pRedisConn;
    }

    RedisConn *GetConn(int ioRole)
    {
        RedisConn *pRedisConn = NULL;
        if (!mHaveSlave) {
            ioRole = MASTER;
        }
        if (MASTER == ioRole) {
            pRedisConn = GetMasterConn();
        } else if (SLAVE == ioRole) {
            pRedisConn = GetSlaveConn();
        } else {
            pRedisConn = NULL;
        }

        return pRedisConn;
    }

    void FreeConn(RedisConn *redisconn)
    {
        if (NULL != redisconn) {
            unsigned int role = redisconn->GetRole();
            if (MASTER == role) {
                XLOCK(mSliceConn.Masterlock);
                mSliceConn.RedisMasterConn.push_back(redisconn);
            }   else if (SLAVE == role) {
                XLOCK(mSliceConn.Slavelock);
                RedisConnPool *pSlave = mSliceConn.RedisSlaveConn[redisconn->GetSlaveIdx()];
                pSlave->push_back(redisconn);
            } else {

            }
        }
    }

    void CloseConnPool()
    {
        {
            XLOCK(mSliceConn.Masterlock);
            RedisConnIter master_iter = mSliceConn.RedisMasterConn.begin();
            for (; master_iter != mSliceConn.RedisMasterConn.end(); ++master_iter) {
                printf("close dbindex:%u  master \r\n", mDbindex);
                redisFree((*master_iter)->getCtx());
                delete *master_iter;
            }
        }

        {
            XLOCK(mSliceConn.Slavelock);
            RedisSlaveGroupIter slave_iter = mSliceConn.RedisSlaveConn.begin();
            for (; slave_iter != mSliceConn.RedisSlaveConn.end(); ++slave_iter) {
                RedisConnPool* pConnPool = (*slave_iter);
                RedisConnIter iter = pConnPool->begin();
                for (; iter != pConnPool->end(); ++iter) {
                    printf("close dbindex:%u slave mSlaveIdx:%u \r\n", mDbindex, (*iter)->GetSlaveIdx());
                    redisFree((*iter)->getCtx());
                    delete *iter;
                }
                delete pConnPool;
            }
        }
        mStatus = REDISDB_DEAD;
    }

    void ConnPoolPing()
    {
        {
            XLOCK(mSliceConn.Masterlock);
            RedisConnIter iter = mSliceConn.RedisMasterConn.begin();
            for (; iter != mSliceConn.RedisMasterConn.end(); ++iter) {
                bool bRet = (*iter)->Ping();
                if (!bRet) {
                    (*iter)->RedisConnect();
                } 
            }
        }

        {
            XLOCK(mSliceConn.Slavelock);
            RedisSlaveGroupIter slave_iter = mSliceConn.RedisSlaveConn.begin();
            for (; slave_iter != mSliceConn.RedisSlaveConn.end(); ++slave_iter) {
                RedisConnPool* pConnPool = (*slave_iter);
                RedisConnIter iter = pConnPool->begin();
                for (; iter != pConnPool->end(); ++iter) {
                    bool bRet = (*iter)->Ping();
                    if (!bRet) {
                        (*iter)->RedisConnect();
                    } 
                }
                delete pConnPool;
            }
        }
    }

    unsigned int GetStatus() const {
        return mStatus;
    }

private:
    RedisSliceConn mSliceConn;
    bool           mHaveSlave;
    unsigned int   mType;          // redis cache pool type
    unsigned int   mDbindex;       // redis DB index
    unsigned int   mStatus;        // redis DB status
};

class RedisCache
{
public:
    RedisCache()
    {
        mCachetype = 0;
        mHashbase  = 0;
        mDBList    = NULL;
    }
    virtual ~RedisCache()
    {

    }

    bool InitDB(const unsigned int cachetype, const unsigned int hashbase)
    {

        mCachetype = cachetype;
        mHashbase  = hashbase;
        if(NULL==mDBList) {
            mDBList    = new RedisDBSlice[hashbase];
        }
        return true;
    }

    bool ConnectRedisDB(const unsigned int cahcetype, const unsigned int dbindex,
                        const char *host, const unsigned int port, const char *passwd,
                        const unsigned int poolsize, unsigned int timeout, unsigned int role)
    {
        mDBList[dbindex].Init(cahcetype, dbindex);
        return mDBList[dbindex].ConnectRedisNodes(cahcetype, dbindex, host, port,
                passwd, poolsize, timeout, role);
    }

    RedisConn *GetConn(const unsigned int dbindex, unsigned int ioRole)
    {
        return mDBList[dbindex].GetConn(ioRole);
    }

    void FreeConn(RedisConn *redisconn)
    {
        return mDBList[redisconn->getdbindex()].FreeConn(redisconn);
    }

    void ClosePool()
    {
        for (unsigned int i = 0; i < mHashbase; i++) {
            mDBList[i].CloseConnPool();
        }
        delete [] mDBList;
        mDBList = NULL;
    }

    void KeepAlive()
    {
        for (unsigned int i = 0; i < mHashbase; i++) {
            mDBList[i].ConnPoolPing();
        }
    }

    unsigned int GetDBStatus(unsigned int dbindex)
    {
        RedisDBSlice *pdbSclice = &mDBList[dbindex];
        if (NULL == pdbSclice) {
            return REDISDB_UNCONN;
        }
        return pdbSclice->GetStatus();
    }

    unsigned int GetHashBase() const
    {
        return mHashbase;
    }

private:
    RedisDBSlice *mDBList;
    unsigned int  mCachetype;
    unsigned int  mHashbase;
};


class RedisPool
{
public:
    RedisPool();
    ~RedisPool();

    bool Init(unsigned int typesize);
    bool setHashBase(unsigned int cachetype, unsigned int hashbase);
    unsigned int getHashBase(unsigned int cachetype);
    bool ConnectRedisDB(unsigned int cachetype,  unsigned int dbindex,
                        const char* host,  unsigned int port, const char* passwd,
                        unsigned int poolsize,  unsigned int timeout, unsigned int role);
    void Release();

    static bool CheckReply(const redisReply* reply);
    static void FreeReply(const redisReply* reply);
    void Keepalive();

    RedisConn *GetConnection(unsigned int cachetype, unsigned int index, unsigned int ioType = MASTER);
    void FreeConnection(RedisConn* redisconn);

private:
    RedisCache     *mRedisCacheList;
    unsigned int    mTypeSize;
};


#endif
