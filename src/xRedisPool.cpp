/*
 * ----------------------------------------------------------------------------
 * Copyright (c) 2013-2021, xSky <guozhw at gmail dot com>
 * All rights reserved.
 * Distributed under GPL license.
 * ----------------------------------------------------------------------------
 */

#include "xRedisPool.h"
#include "hiredis.h"
#include "xRedisLog.h"
#include <time.h>

namespace xrc {

RedisPool::RedisPool()
{
    mRedisGroupList = NULL;
    mTypeSize = 0;
    srand((unsigned)time(NULL));
}

RedisPool::~RedisPool() { }

bool RedisPool::Init(uint32_t typesize)
{
    mTypeSize = typesize;
    if (mTypeSize > MAX_REDIS_CACHE_TYPE) {
        return false;
    }

    mRedisGroupList = new RedisGroup[mTypeSize];
    return mRedisGroupList != NULL;
}

bool RedisPool::SetHashBase(uint32_t cachetype, uint32_t hashbase)
{
    if ((hashbase > MAX_REDIS_DB_HASHBASE) || (cachetype > mTypeSize - 1)) {
        return false;
    }
    bool bRet = mRedisGroupList[cachetype].InitDB(cachetype, hashbase);
    return bRet;
}

uint32_t RedisPool::GetHashBase(uint32_t cachetype)
{
    if ((cachetype > mTypeSize) || (cachetype > MAX_REDIS_CACHE_TYPE)) {
        return 0;
    }
    return mRedisGroupList[cachetype].GetHashBase();
}

void RedisPool::Keepalive()
{
    for (uint32_t i = 0; i < mTypeSize; i++) {
        if (mRedisGroupList[i].GetHashBase() > 0) {
            mRedisGroupList[i].KeepAlive();
        }
    }
}

bool RedisPool::CheckReply(const redisReply* reply)
{
    if (NULL == reply) {
        return false;
    }

    switch (reply->type) {
    case REDIS_REPLY_STRING: {
        return true;
    }
    case REDIS_REPLY_ARRAY: {
        return true;
    }
    case REDIS_REPLY_INTEGER: {
        return true;
    }
    case REDIS_REPLY_NIL: {
        return false;
    }
    case REDIS_REPLY_STATUS: {
        return true;
    }
    case REDIS_REPLY_ERROR: {
        return false;
    }
    default: {
        return false;
    }
    }

    return false;
}

void RedisPool::FreeReply(const redisReply* reply)
{
    if (NULL != reply) {
        freeReplyObject((void*)reply);
    }
}

bool RedisPool::ConnectRedisGroup(uint32_t cachetype, uint32_t dbindex,
    const std::string& host, uint32_t port,
    const std::string& passwd, uint32_t poolsize,
    uint32_t timeout, uint32_t role)
{
    if ((0 == host.length()) || (cachetype > MAX_REDIS_CACHE_TYPE) || (dbindex > MAX_REDIS_DB_HASHBASE)
        || (cachetype > mTypeSize - 1) || (role > SLAVE) || (poolsize > MAX_REDIS_CONN_POOLSIZE)) {
        xredis_error("ConnectRedisDB cachetype:%u dbindex:%u host:%s port:%u passwd:%s poolsize:%u timeout:%u role:%u",
            cachetype, dbindex, host.c_str(), port, passwd.c_str(), poolsize, timeout, role);
        return false;
    }

    return mRedisGroupList[cachetype].ConnectRedisGroup(
        cachetype, dbindex, host, port, passwd, poolsize, timeout, role);
}

void RedisPool::Release()
{
    for (uint32_t i = 0; i < mTypeSize; i++) {
        if (mRedisGroupList[i].GetHashBase() > 0) {
            mRedisGroupList[i].ClosePool();
        }
    }
    delete[] mRedisGroupList;
}

RedisConnection* RedisPool::GetConnection(uint32_t cachetype, uint32_t dbindex,
    uint32_t ioType)
{
    RedisConnection* pRedisConn = NULL;

    if ((cachetype > mTypeSize) || (dbindex > mRedisGroupList[cachetype].GetHashBase()) || (ioType > SLAVE)) {
        return NULL;
    }

    RedisGroup* pRedisCache = &mRedisGroupList[cachetype];
    pRedisConn = pRedisCache->GetConn(dbindex, ioType);

    return pRedisConn;
}

void RedisPool::FreeConnection(RedisConnection* redisconn)
{
    if (NULL != redisconn) {
        mRedisGroupList[redisconn->GetType()].FreeConn(redisconn);
    }
}

RedisConnection::RedisConnection()
{
    mCtx = NULL;
    mPort = 0;
    mTimeout = 0;
    mPoolsize = 0;
    mType = 0;
    mSliceIndex = 0;
    mConnStatus = false;
}

RedisConnection::~RedisConnection() { }

redisContext* RedisConnection::ConnectWithTimeout()
{
    struct timeval timeoutVal;
    timeoutVal.tv_sec = mTimeout;
    timeoutVal.tv_usec = 0;

    redisContext* ctx = NULL;
    ctx = redisConnectWithTimeout(mHost.c_str(), mPort, timeoutVal);
    if (NULL == ctx || ctx->err) {
        xredis_info("ConnectWithTimeout auth failed dbindex:%u host:%s port:%u passwd:%s poolsize:%u timeout:%u role:%u",
            mSliceIndex, mHost.c_str(), mPort, mPass.c_str(), mPoolsize, mTimeout, mRole);
        if (NULL != ctx) {
            redisFree(ctx);
            ctx = NULL;
        } else {
        }
    }

    return ctx;
}

bool RedisConnection::Auth()
{
    bool bRet = false;
    if (0 == mPass.length()) {
        bRet = true;
    } else {
        redisReply* reply = static_cast<redisReply*>(redisCommand(mCtx, "AUTH %s", mPass.c_str()));
        if ((NULL == reply) || (strcasecmp(reply->str, "OK") != 0)) {
            xredis_error("auth failed dbindex:%u host:%s port:%u passwd:%s poolsize:%u timeout:%u role:%u",
                mSliceIndex, mHost.c_str(), mPort, mPass.c_str(), mPoolsize, mTimeout, mRole);
            bRet = false;
        } else {
            xredis_info("auth success dbindex:%u host:%s port:%u passwd:%s poolsize:%u timeout:%u role:%u",
                mSliceIndex, mHost.c_str(), mPort, mPass.c_str(), mPoolsize, mTimeout, mRole);
            bRet = true;
        }
        freeReplyObject(reply);
    }

    return bRet;
}

bool RedisConnection::RedisConnect()
{
    bool bRet = false;
    if (NULL != mCtx) {
        redisFree(mCtx);
        mCtx = NULL;
    }

    mCtx = ConnectWithTimeout();
    if (NULL == mCtx) {
        bRet = false;
    } else {
        bRet = Auth();
        mConnStatus = bRet;
    }

    return bRet;
}

bool RedisConnection::RedisReConnect()
{
    if (NULL == mCtx) {
        return false;
    }

    bool bRet = false;
    redisContext* tmp_ctx = ConnectWithTimeout();
    if (NULL == tmp_ctx) {
        xredis_warn("RedisReConnect failed dbindex:%u host:%s port:%u passwd:%s poolsize:%u timeout:%u role:%u",
            mSliceIndex, mHost.c_str(), mPort, mPass.c_str(), mPoolsize, mTimeout, mRole);
        bRet = false;
    } else {
        redisFree(mCtx);
        mCtx = tmp_ctx;
        bRet = Auth();
    }

    mConnStatus = bRet;
    return bRet;
}

bool RedisConnection::Ping()
{
    redisReply* reply = static_cast<redisReply*>(redisCommand(mCtx, "PING"));
    bool bRet = (NULL != reply) && (reply->str) && (strcasecmp(reply->str, "PONG") == 0);
    mConnStatus = bRet;
    if (bRet) {
        freeReplyObject(reply);
    }
    if (bRet) {
        xredis_warn("Ping failed dbindex:%u host:%s port:%u passwd:%s poolsize:%u timeout:%u role:%u",
            mSliceIndex, mHost.c_str(), mPort, mPass.c_str(), mPoolsize, mTimeout, mRole);
    }
    return bRet;
}

void RedisConnection::Init(uint32_t cahcetype, uint32_t dbindex,
    const std::string& host, uint32_t port,
    const std::string& pass, uint32_t poolsize,
    uint32_t timeout, uint32_t role, uint32_t slaveidx)
{
    mType = cahcetype;
    mSliceIndex = dbindex;
    mHost = host;
    mPort = port;
    mPass = pass;
    mPoolsize = poolsize;
    mTimeout = timeout;
    mRole = role;
    mSlaveIdx = slaveidx;
}

RedisSlice::RedisSlice()
{
    mType = 0;
    mSliceindex = 0;
    mStatus = 0;
    mHaveSlave = false;
}

RedisSlice::~RedisSlice() { }

void RedisSlice::Init(uint32_t cahcetype, uint32_t dbindex)
{
    mType = cahcetype;
    mSliceindex = dbindex;
}

bool RedisSlice::ConnectRedisSlice(uint32_t cahcetype, uint32_t dbindex,
    const std::string& host, uint32_t port,
    const std::string& passwd,
    uint32_t poolsize, uint32_t timeout,
    int32_t role)
{
    bool bRet = false;
    if ((host.empty()) || (cahcetype > MAX_REDIS_CACHE_TYPE) || (dbindex > MAX_REDIS_DB_HASHBASE) || (poolsize > MAX_REDIS_CONN_POOLSIZE)) {
        return false;
    }

    try {
        if (MASTER == role) {
            XLOCK(mSliceConn.MasterLock);
            for (uint32_t i = 0; i < poolsize; ++i) {
                RedisConnection* pRedisconn = new RedisConnection;
                if (NULL == pRedisconn) {
                    continue;
                }

                pRedisconn->Init(cahcetype, dbindex, host, port, passwd, poolsize,
                    timeout, role, 0);
                if (pRedisconn->RedisConnect()) {
                    mSliceConn.RedisMasterConnection.push_back(pRedisconn);
                    mStatus = REDISDB_WORKING;
                    bRet = true;
                } else {
                    delete pRedisconn;
                }
            }

        } else if (SLAVE == role) {
            XLOCK(mSliceConn.SlaveLock);
            RedisConnectionPool* pSlaveNode = new RedisConnectionPool;
            int32_t slave_idx = mSliceConn.RedisSlaveConnection.size();
            for (uint32_t i = 0; i < poolsize; ++i) {
                RedisConnection* pRedisconn = new RedisConnection;
                if (NULL == pRedisconn) {
                    continue;
                }

                pRedisconn->Init(cahcetype, dbindex, host, port, passwd, poolsize,
                    timeout, role, slave_idx);
                if (pRedisconn->RedisConnect()) {
                    pSlaveNode->push_back(pRedisconn);
                    bRet = true;
                } else {
                    delete pRedisconn;
                }
            }
            mSliceConn.RedisSlaveConnection.push_back(pSlaveNode);
            mHaveSlave = true;
        } else {
            bRet = false;
        }

    } catch (...) {
        return false;
    }

    return bRet;
}

RedisConnection* RedisSlice::GetMasterConn()
{
    RedisConnection* pRedisConn = NULL;
    XLOCK(mSliceConn.MasterLock);
    if (!mSliceConn.RedisMasterConnection.empty()) {
        pRedisConn = mSliceConn.RedisMasterConnection.front();
        mSliceConn.RedisMasterConnection.pop_front();
    } else {
        mStatus = REDISDB_DEAD;
    }
    return pRedisConn;
}

RedisConnection* RedisSlice::GetSlaveConn()
{
    RedisConnection* pRedisConn = NULL;
    XLOCK(mSliceConn.SlaveLock);
    if (!mSliceConn.RedisSlaveConnection.empty()) {
        size_t slave_cnt = mSliceConn.RedisSlaveConnection.size();
        uint32_t idx = rand() % slave_cnt;
        RedisConnectionPool* pSlave = mSliceConn.RedisSlaveConnection[idx];
        pRedisConn = pSlave->front();
        pSlave->pop_front();
        // if (idx != pRedisConn->GetSlaveIdx()) {
        //}
    }
    return pRedisConn;
}

RedisConnection* RedisSlice::GetConn(int32_t ioRole)
{
    RedisConnection* pRedisConn = NULL;
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

void RedisSlice::FreeConn(RedisConnection* redisconn)
{
    if (NULL != redisconn) {
        uint32_t role = redisconn->GetRole();
        if (MASTER == role) {
            XLOCK(mSliceConn.MasterLock);
            mSliceConn.RedisMasterConnection.push_back(redisconn);
        } else if (SLAVE == role) {
            XLOCK(mSliceConn.SlaveLock);
            RedisConnectionPool* pSlave = mSliceConn.RedisSlaveConnection[redisconn->GetSlaveIdx()];
            pSlave->push_back(redisconn);
        } else {
        }
    }
}

void RedisSlice::CloseConnPool()
{
    {
        XLOCK(mSliceConn.MasterLock);
        RedisConnectionIter master_iter = mSliceConn.RedisMasterConnection.begin();
        for (; master_iter != mSliceConn.RedisMasterConnection.end(); ++master_iter) {
            redisFree((*master_iter)->GetCtx());
            delete *master_iter;
        }
    }

    {
        XLOCK(mSliceConn.SlaveLock);
        RedisSlaveGroupIter slave_iter = mSliceConn.RedisSlaveConnection.begin();
        for (; slave_iter != mSliceConn.RedisSlaveConnection.end(); ++slave_iter) {
            RedisConnectionPool* pConnPool = (*slave_iter);
            RedisConnectionIter iter = pConnPool->begin();
            for (; iter != pConnPool->end(); ++iter) {
                redisFree((*iter)->GetCtx());
                delete *iter;
            }
            delete pConnPool;
        }
    }

    mStatus = REDISDB_DEAD;
}

void RedisSlice::ConnPoolPing()
{
    {
        XLOCK(mSliceConn.MasterLock);
        RedisConnectionIter master_iter = mSliceConn.RedisMasterConnection.begin();
        for (; master_iter != mSliceConn.RedisMasterConnection.end(); ++master_iter) {
            bool bRet = (*master_iter)->Ping();
            if (!bRet) {
                (*master_iter)->RedisReConnect();
            } else {
            }
        }
    }

    {
        XLOCK(mSliceConn.SlaveLock);
        RedisSlaveGroupIter slave_iter = mSliceConn.RedisSlaveConnection.begin();
        for (; slave_iter != mSliceConn.RedisSlaveConnection.end(); ++slave_iter) {
            RedisConnectionPool* pConnPool = (*slave_iter);
            RedisConnectionIter iter = pConnPool->begin();
            for (; iter != pConnPool->end(); ++iter) {
                bool bRet = (*iter)->Ping();
                if (!bRet) {
                    (*iter)->RedisReConnect();
                } else {
                }
            }
        }
    }
}

uint32_t RedisSlice::GetStatus() const { return mStatus; }

RedisGroup::RedisGroup()
{
    mCachetype = 0;
    mHashbase = 0;
    mSliceList = NULL;
}

RedisGroup::~RedisGroup() { }

bool RedisGroup::InitDB(uint32_t cachetype, uint32_t hashbase)
{
    mCachetype = cachetype;
    mHashbase = hashbase;
    if (NULL == mSliceList) {
        mSliceList = new RedisSlice[hashbase];
    }

    return true;
}

bool RedisGroup::ConnectRedisGroup(uint32_t cahcetype, uint32_t dbindex,
    const std::string& host, uint32_t port,
    const std::string& passwd, uint32_t poolsize,
    uint32_t timeout, uint32_t role)
{
    mSliceList[dbindex].Init(cahcetype, dbindex);
    return mSliceList[dbindex].ConnectRedisSlice(cahcetype, dbindex, host, port,
        passwd, poolsize, timeout, role);
}

void RedisGroup::ClosePool()
{
    for (uint32_t i = 0; i < mHashbase; i++) {
        mSliceList[i].CloseConnPool();
    }
    delete[] mSliceList;
    mSliceList = NULL;
}

void RedisGroup::KeepAlive()
{
    for (uint32_t i = 0; i < mHashbase; i++) {
        mSliceList[i].ConnPoolPing();
    }
}

uint32_t RedisGroup::GetDBStatus(uint32_t dbindex)
{
    RedisSlice* pdbSclice = &mSliceList[dbindex];
    if (NULL == pdbSclice) {
        return REDISDB_UNCONN;
    }
    return pdbSclice->GetStatus();
}

void RedisGroup::FreeConn(RedisConnection* redisconn)
{
    return mSliceList[redisconn->GetdbIndex()].FreeConn(redisconn);
}

RedisConnection* RedisGroup::GetConn(uint32_t dbindex, uint32_t ioRole)
{
    return mSliceList[dbindex].GetConn(ioRole);
}

uint32_t RedisGroup::GetHashBase() const { return mHashbase; }
} // namespace xrc
