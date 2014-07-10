
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

    if (mTypeSize>MAX_REDIS_CACHE_TYPE)
    {
        printf("mTypeSize:%u error ", typesize);
        return false;
    }

    printf("typesize:%u \r\n", typesize);

    mRedisCacheList = new RedisCache[mTypeSize];
    return mRedisCacheList!=NULL;
}

bool RedisPool::setHashBase(const unsigned int cachetype, const unsigned int hashbase) {
    if ( hashbase>MAX_REDIS_DB_HASHBASE) {
        return false;
    }
    printf("cachetype:%u hashbase:%u \r\n", cachetype, hashbase);

    bool bRet = mRedisCacheList[cachetype].InitDB(cachetype, hashbase);
    return bRet;
}

unsigned int RedisPool::getHashBase(const unsigned int cachetype) {
    if ( cachetype>MAX_REDIS_CACHE_TYPE) {
        return 0;
    }
    return mRedisCacheList[cachetype].GetHashBase();
}

void RedisPool::KeepAlive() {
    for(unsigned int i=0; i<mTypeSize; i++) {
        if (mRedisCacheList[i].GetHashBase()>0){
            printf("mTypeSize:%u\n", i);
            mRedisCacheList[i].KeepAlive();
            printf("RedisPool::KeepAlive\n");
        }
    }
}

bool RedisPool::CheckReply(const redisReply *reply){
    if(NULL==reply) {
        printf("error, reply is NULL \r\n");
        return false;
    }

    printf("DEBBUG %d:%s %lld %lu \r\n", reply->type, reply->str, reply->integer, reply->elements);

    switch(reply->type){
    case REDIS_REPLY_STRING:{
            return true;
        }
    case REDIS_REPLY_ARRAY:{
            return (strcasecmp(reply->str,"OK") == 0)?true:false;
        }
    case REDIS_REPLY_INTEGER:{
            return true;
        }
    case REDIS_REPLY_NIL:{
            printf("REDIS_REPLY_NIL-%s \r\n", reply->str);
            return false;
        }
    case REDIS_REPLY_STATUS:{
            return (strcasecmp(reply->str,"OK") == 0)?true:false;
        }
    case REDIS_REPLY_ERROR:{
            printf("REDIS_REPLY_ERROR-%s \r\n", reply->str);
            return false;
        }
    default:{
            printf("ERROR %d:%s \r\n", reply->type, reply->str);
            return false;
        }
    }

    return false;
}

void RedisPool::FreeReply(const redisReply *reply){
    if (NULL!=reply) {
        freeReplyObject((void*)reply);
    } else {
        printf("RedisPool::FreeReply error \r\n");
    }
}

bool RedisPool::ConnectRedisDB( unsigned int cahcetype,  unsigned int dbindex,
    const char *host,  unsigned int port, const char *passwd,
    unsigned int poolsize,  unsigned int timeout){
    if((NULL==host)
        ||(cahcetype>MAX_REDIS_CACHE_TYPE)
        ||(dbindex>MAX_REDIS_DB_HASHBASE)
        ||(poolsize>MAX_REDIS_CONN_POOLSIZE)){
            printf("error argv: cahcetype:%u dbindex:%u host:%s port:%u poolsize:%u timeout:%u \r\n", 
                cahcetype, dbindex, host, port, poolsize, timeout);
            return false;
    }

    return mRedisCacheList[cahcetype].ConnectRedisDB(cahcetype, dbindex, host, port,
        passwd, poolsize, timeout);
}

void RedisPool::Release(){
    printf("\n");
    for(unsigned int i=0; i<mTypeSize; i++) {
        if (mRedisCacheList[i].GetHashBase()>0) {
            mRedisCacheList[i].ClosePool();
            printf("RedisPool::Release()\n");
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
            printf("RedisPool::GetConnection()  error pthread_id=%u \n", (unsigned int)pthread_self());
        }
        usleep(1000);
    }

    return pRedisConn;
}

void RedisPool::FreeConnection(RedisConn *_redisconn){
    if (NULL!=_redisconn) {
        mRedisCacheList[_redisconn->GetType()].FreeConn(_redisconn);
    }
}



