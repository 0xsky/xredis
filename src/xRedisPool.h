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
using namespace std;

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

    void Init(unsigned int cahcetype,
              unsigned int dbindex,
              const std::string&  host,
              unsigned int  port,
              const std::string&  pass,
              unsigned int poolsize,
              unsigned int timeout,
              unsigned int role,
              unsigned int slaveidx
             );

    bool RedisConnect();
    bool RedisReConnect();
    bool Ping();

    redisContext   *getCtx() const     { return mCtx; }
    unsigned int    getdbindex() const { return mDbindex; }
    unsigned int    GetType() const   { return  mType; }
    unsigned int    GetRole() const   { return  mRole; }
    unsigned int    GetSlaveIdx() const   { return  mSlaveIdx; }
    bool            GetConnstatus() const   { return  mConnStatus; }
    
private:
    bool auth();
    redisContext * ConnectWithTimeout();

private:
    // redis connector context
    redisContext   *mCtx;
    string          mHost;          // redis host
    unsigned int    mPort;          // redis sever port
    string          mPass;          // redis server password
    unsigned int    mTimeout;       // connect timeout second
    unsigned int    mPoolsize;      // connect pool size for each redis DB
    unsigned int    mType;          // redis cache pool type
    unsigned int    mDbindex;       // redis DB index
    unsigned int    mRole;          // redis role
    unsigned int    mSlaveIdx;      // the index in the slave group
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

    void Init( unsigned int  cahcetype, unsigned int  dbindex);
    // 连到到一个REDIS服务节点
    bool ConnectRedisNodes(unsigned int cahcetype, unsigned int dbindex,
                           const std::string& host,  unsigned int port, const std::string& passwd,
                           unsigned int poolsize,  unsigned int timeout, int role);

    RedisConn *GetMasterConn();
    RedisConn *GetSlaveConn();
    RedisConn *GetConn(int ioRole);
    void FreeConn(RedisConn *redisconn);
    void CloseConnPool();
    void ConnPoolPing();
    unsigned int GetStatus() const;

private:
    RedisSliceConn mSliceConn;
    bool           mHaveSlave;
    unsigned int   mType;          // redis cache pool type
    unsigned int   mDbindex;       // redis slice index
    unsigned int   mStatus;        // redis DB status
};

class RedisCache
{
public:
    RedisCache();
    virtual ~RedisCache();

    bool InitDB( unsigned int cachetype,  unsigned int hashbase);
    bool ConnectRedisDB( unsigned int cahcetype,  unsigned int dbindex,
                        const char *host,  unsigned int port, const char *passwd,
                         unsigned int poolsize, unsigned int timeout, unsigned int role);

    RedisConn *GetConn( unsigned int dbindex, unsigned int ioRole);
    void FreeConn(RedisConn *redisconn);
    void ClosePool();
    void KeepAlive();
    unsigned int GetDBStatus(unsigned int dbindex);
    unsigned int GetHashBase() const;

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
    static bool CheckReply(const redisReply* reply);
    static void FreeReply(const redisReply* reply);
    
    RedisConn *GetConnection(unsigned int cachetype, unsigned int index, unsigned int ioType = MASTER);
    void FreeConnection(RedisConn* redisconn);
    
    void Keepalive();
    void Release();
private:
    RedisCache     *mRedisCacheList;
    unsigned int    mTypeSize;
};


#endif
