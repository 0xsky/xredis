/*
 * ----------------------------------------------------------------------------
 * Copyright (c) 2013-2014, xSky <guozhw at gmail dot com>
 * All rights reserved.
 * Distributed under GPL license.
 * ----------------------------------------------------------------------------
 */

#ifndef _XREDIS_POOL_H_
#define _XREDIS_POOL_H_

#include "hiredis.h"
#include "xLock.h"
#include <string.h>
#include <string>
#include <list>
#include "xRedisClient.h"

namespace xrc
{

#define MAX_REDIS_CONN_POOLSIZE     128      // 每个DB最大连接数
#define MAX_REDIS_CACHE_TYPE        128      // 最大支持的CACHE种类数
#define MAX_REDIS_DB_HASHBASE       128      // 最大HASH分库基数

#define GET_CONNECT_ERROR       "get connection error"
#define CONNECT_CLOSED_ERROR    "redis connection be closed"

#ifdef WIN32
    #define   strcasecmp   stricmp
    #define   strncasecmp  strnicmp
#endif

enum {
    REDISDB_UNCONN,
    REDISDB_WORKING,
    REDISDB_DEAD
};

class RedisConn
{
public:
    RedisConn();
    ~RedisConn();

    void Init(uint32_t cahcetype,
              uint32_t dbindex,
              const std::string&  host,
              uint32_t  port,
              const std::string&  pass,
              uint32_t poolsize,
              uint32_t timeout,
              uint32_t role,
              uint32_t slaveidx
             );

    bool RedisConnect();
    bool RedisReConnect();
    bool Ping();

    redisContext   *getCtx() const     { return mCtx; }
    uint32_t    getdbindex() const { return mDbindex; }
    uint32_t    GetType() const   { return  mType; }
    uint32_t    GetRole() const   { return  mRole; }
    uint32_t    GetSlaveIdx() const   { return  mSlaveIdx; }
    bool            GetConnstatus() const   { return  mConnStatus; }
    
private:
    bool auth();
    redisContext * ConnectWithTimeout();

private:
    // redis connector context
    redisContext   *mCtx;
    std::string     mHost;          // redis host
    uint32_t    mPort;          // redis sever port
    std::string     mPass;          // redis server password
    uint32_t    mTimeout;       // connect timeout second
    uint32_t    mPoolsize;      // connect pool size for each redis DB
    uint32_t    mType;          // redis cache pool type
    uint32_t    mDbindex;       // redis DB index
    uint32_t    mRole;          // redis role
    uint32_t    mSlaveIdx;      // the index in the slave group
    bool            mConnStatus;    // redis connection status
};

typedef std::list<RedisConn*> RedisConnPool;
typedef std::list<RedisConn*>::iterator RedisConnIter;

typedef std::vector<RedisConnPool*> RedisSlaveGroup;
typedef std::vector<RedisConnPool*>::iterator RedisSlaveGroupIter;

typedef struct _RedisDBSlice_Conn_ {
    RedisConnPool   RedisMasterConn;
    RedisSlaveGroup RedisSlaveConn;
    xLock           MasterLock;
    xLock           SlaveLock;
} RedisSliceConn;


class RedisDBSlice
{
public:
    RedisDBSlice();
    ~RedisDBSlice();

    void Init( uint32_t  cahcetype, uint32_t  dbindex);
    // 连到到一个REDIS服务节点
    bool ConnectRedisNodes(uint32_t cahcetype, uint32_t dbindex,
                           const std::string& host,  uint32_t port, const std::string& passwd,
                           uint32_t poolsize,  uint32_t timeout, int32_t role);

    RedisConn *GetMasterConn();
    RedisConn *GetSlaveConn();
    RedisConn *GetConn(int32_t ioRole);
    void FreeConn(RedisConn *redisconn);
    void CloseConnPool();
    void ConnPoolPing();
    uint32_t GetStatus() const;

private:
    RedisSliceConn mSliceConn;
    bool           mHaveSlave;
    uint32_t   mType;          // redis cache pool type
    uint32_t   mDbindex;       // redis slice index
    uint32_t   mStatus;        // redis DB status
};

class RedisCache
{
public:
    RedisCache();
    virtual ~RedisCache();

    bool InitDB( uint32_t cachetype,  uint32_t hashbase);
    bool ConnectRedisDB( uint32_t cahcetype,  uint32_t dbindex,
        const std::string& host, uint32_t port, const std::string& passwd,
        uint32_t poolsize, uint32_t timeout, uint32_t role);

    RedisConn *GetConn( uint32_t dbindex, uint32_t ioRole);
    void FreeConn(RedisConn *redisconn);
    void ClosePool();
    void KeepAlive();
    uint32_t GetDBStatus(uint32_t dbindex);
    uint32_t GetHashBase() const;

private:
    RedisDBSlice *mDBList;
    uint32_t  mCachetype;
    uint32_t  mHashbase;
};


class RedisPool
{
public:
    RedisPool();
    ~RedisPool();

    bool Init(uint32_t typesize);
    bool setHashBase(uint32_t cachetype, uint32_t hashbase);
    uint32_t getHashBase(uint32_t cachetype);
    bool ConnectRedisDB(uint32_t cachetype,  uint32_t dbindex,
        const std::string& host, uint32_t port, const std::string& passwd,
                        uint32_t poolsize,  uint32_t timeout, uint32_t role);
    static bool CheckReply(const redisReply* reply);
    static void FreeReply(const redisReply* reply);
    
    RedisConn *GetConnection(uint32_t cachetype, uint32_t index, uint32_t ioType = MASTER);
    void FreeConnection(RedisConn* redisconn);
    
    void Keepalive();
    void Release();
private:
    RedisCache     *mRedisCacheList;
    uint32_t    mTypeSize;
};


}

#endif
