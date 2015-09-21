/*
 * ----------------------------------------------------------------------------
 * Copyright (c) 2013-2014, xSky <guozhw at gmail dot com>
 * All rights reserved.
 * Distributed under GPL license.
 * ----------------------------------------------------------------------------
 */

#include "xRedisPool.h"
#include "hiredis.h"


RedisPool::RedisPool() {
    mRedisCacheList = NULL;
    mTypeSize=0;
}

RedisPool::~RedisPool() {

}

RedisPool *RedisPool::GetInstance() {
    static RedisPool redispool;
    return &redispool;
}

bool RedisPool::Init(const unsigned int typesize) {
    mTypeSize = typesize;
    if (mTypeSize>MAX_REDIS_CACHE_TYPE){
        return false;
    }

    mRedisCacheList = new RedisCache[mTypeSize];
    return mRedisCacheList!=NULL;
}

bool RedisPool::setHashBase(const unsigned int cachetype, const unsigned int hashbase) {
    if ( (hashbase>MAX_REDIS_DB_HASHBASE)||(cachetype>mTypeSize-1)) {
        return false;
    }

    bool bRet = mRedisCacheList[cachetype].InitDB(cachetype, hashbase);
    return bRet;
}

unsigned int RedisPool::getHashBase(const unsigned int cachetype) {
    if ((cachetype > mTypeSize) || (cachetype > MAX_REDIS_CACHE_TYPE)) {
        return 0;
    }
    return mRedisCacheList[cachetype].GetHashBase();
}

void RedisPool::KeepAlive() {
    for(unsigned int i=0; i<mTypeSize; i++) {
        if (mRedisCacheList[i].GetHashBase()>0){
            mRedisCacheList[i].KeepAlive();
        }
    }
}

bool RedisPool::CheckReply(const redisReply *reply){
    if(NULL==reply) {
        return false;
    }

    switch(reply->type){
    case REDIS_REPLY_STRING:{
            return true;
        }
    case REDIS_REPLY_ARRAY:{
        return true;
        }
    case REDIS_REPLY_INTEGER:{
            return true;
        }
    case REDIS_REPLY_NIL:{
            return false;
        }
    case REDIS_REPLY_STATUS:{
            return (strcasecmp(reply->str,"OK") == 0)?true:false;
        }
    case REDIS_REPLY_ERROR:{
            return false;
        }
    default:{
            return false;
        }
    }

    return false;
}

void RedisPool::FreeReply(const redisReply *reply){
    if (NULL!=reply) {
        freeReplyObject((void*)reply);
    }
}

bool RedisPool::ConnectRedisDB( unsigned int cahcetype,  unsigned int dbindex,
    const char *host,  unsigned int port, const char *passwd,
    unsigned int poolsize,  unsigned int timeout){
    if((NULL==host)
        ||(cahcetype>MAX_REDIS_CACHE_TYPE)
        ||(dbindex>MAX_REDIS_DB_HASHBASE)
        || (cahcetype>mTypeSize - 1)
        ||(poolsize>MAX_REDIS_CONN_POOLSIZE)){
            return false;
    }

    return mRedisCacheList[cahcetype].ConnectRedisDB(cahcetype, dbindex, host, port,
        passwd, poolsize, timeout);
}

void RedisPool::Release(){
    for(unsigned int i=0; i<mTypeSize; i++) {
        if (mRedisCacheList[i].GetHashBase()>0) {
            mRedisCacheList[i].ClosePool();
        }
    }
    delete [] mRedisCacheList;
}

RedisConn *RedisPool::GetConnection(const unsigned int cahcetype, const unsigned int dbindex){
    RedisConn *pRedisConn = NULL;

    if ( (cahcetype>mTypeSize)||(dbindex>mRedisCacheList[cahcetype].GetHashBase())){
        return NULL;
    }

    RedisCache *pRedisCache = &mRedisCacheList[cahcetype];
    if (REDISDB_WORKING!=pRedisCache->GetDBStatus(dbindex)) {
        return NULL;
    }

    while (1) {
        pRedisConn = mRedisCacheList[cahcetype].GetConn(dbindex);
        if (NULL!=pRedisConn) {
            break;
        } else {

        }
        usleep(1000);
    }

    return pRedisConn;
}

void RedisPool::FreeConnection(RedisConn *redisconn){
    if (NULL!=redisconn) {
        mRedisCacheList[redisconn->GetType()].FreeConn(redisconn);
    }
}



