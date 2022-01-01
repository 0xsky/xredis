/*
 * ----------------------------------------------------------------------------
 * Copyright (c) 2013-2021, xSky <guozhw at gmail dot com>
 * All rights reserved.
 * Distributed under GPL license.
 * ----------------------------------------------------------------------------
 */

#include "xRedisClient.h"
#include "xRedisLog.h"
#include "xRedisPool.h"

using namespace xrc;

SliceIndex::SliceIndex()
{
    mType = 0;
    mIndex = 0;
    mStrerr = NULL;
    mClient = NULL;
    mIOtype = MASTER;
    mIOFlag = false;
}

SliceIndex::SliceIndex(xRedisClient* xredisclient, uint32_t type)
{
    mType = type;
    mIndex = 0;
    mStrerr = NULL;
    mClient = xredisclient;
    mIOtype = MASTER;
    mIOFlag = false;
}
SliceIndex::~SliceIndex()
{
    if (NULL != mStrerr) {
        delete[] mStrerr;
        mStrerr = NULL;
    }
}

bool SliceIndex::Create(const char* key, HASHFUN fun)
{
    uint32_t hashbase = mClient->GetRedisPool()->GetHashBase(mType);
    if ((NULL != fun) && (hashbase > 0)) {
        mIndex = fun(key) % hashbase;
        return true;
    }
    return false;
}

bool SliceIndex::CreateByID(int64_t id)
{
    uint32_t hashbase = mClient->GetRedisPool()->GetHashBase(mType);
    if (hashbase > 0) {
        mIndex = id % hashbase;
        return true;
    }
    return false;
}

void SliceIndex::IOtype(uint32_t iotype) { mIOtype = iotype; }

void SliceIndex::SetIOMaster()
{
    mIOtype = MASTER;
    mIOFlag = true;
}

bool SliceIndex::SetErrInfo(const char* info, int32_t len)
{
    if (NULL == info) {
        return false;
    }
    if (NULL == mStrerr) {
        mStrerr = new char[len + 1];
    }
    if (NULL != mStrerr) {
        strncpy(mStrerr, info, len);
        mStrerr[len] = '\0';
        return true;
    }
    return false;
}

unsigned int SliceIndex::APHash(const char* str)
{
    unsigned int hash = 0;
    int i;
    for (i = 0; *str; i++) {
        if ((i & 1) == 0) {
            hash ^= ((hash << 7) ^ (*str++) ^ (hash >> 3));
        } else {
            hash ^= (~((hash << 11) ^ (*str++) ^ (hash >> 5)));
        }
    }
    return (hash & 0x7FFFFFFF);
}

xRedisClient::xRedisClient() { mRedisPool = NULL; }

xRedisClient::~xRedisClient() { Release(); }

bool xRedisClient::Init(uint32_t maxtype)
{
    if (NULL == mRedisPool) {
        mRedisPool = new RedisPool;
        if (NULL == mRedisPool) {
            return false;
        }

        return mRedisPool->Init(maxtype);
    }
    return false;
}

void xRedisClient::Release()
{
    if (NULL != mRedisPool) {
        mRedisPool->Release();
        delete mRedisPool;
        mRedisPool = NULL;
    }
}

void xRedisClient::Keepalive()
{
    if (NULL != mRedisPool) {
        mRedisPool->Keepalive();
    }
}

void xRedisClient::SetLogLevel(uint32_t level, void (*emit)(int level, const char* line))
{
    set_log_level(level, emit);
}

inline RedisPool* xRedisClient::GetRedisPool() { return mRedisPool; }

void xRedisClient::FreeReply(const rReply* reply)
{
    RedisPool::FreeReply((redisReply*)reply);
}

bool xRedisClient::ConnectRedisCache(const RedisNode* redisnodelist,
    uint32_t nodecount, uint32_t hashbase,
    uint32_t cachetype)
{
    if (NULL == mRedisPool) {
        xredis_error("RedisPool is NULL");
        return false;
    }

    if (!mRedisPool->SetHashBase(cachetype, hashbase)) {
        xredis_error("setHashBase error");
        return false;
    }

    for (uint32_t n = 0; n < nodecount; n++) {
        const RedisNode* pNode = &redisnodelist[n];
        if (NULL == pNode) {
            return false;
        }

        bool bRet = mRedisPool->ConnectRedisGroup(
            cachetype, pNode->dbindex, pNode->host, pNode->port, pNode->passwd,
            pNode->poolsize, pNode->timeout, pNode->role);
        if (!bRet) {
            return false;
        }
    }

    return true;
}

void xRedisClient::SetErrInfo(const SliceIndex& index, void* p)
{
    if (NULL == p) {
        SetErrString(index, CONNECT_CLOSED_ERROR, ::strlen(CONNECT_CLOSED_ERROR));
    } else {
        redisReply* reply = (redisReply*)p;
        SetErrString(index, reply->str, reply->len);
    }
}

void xRedisClient::SetErrString(const SliceIndex& index, const char* str,
    int32_t len)
{
    SliceIndex& dbindex = const_cast<SliceIndex&>(index);
    dbindex.SetErrInfo(str, len);
}

void xRedisClient::SetIOtype(const SliceIndex& index, uint32_t iotype,
    bool ioflag)
{
    SliceIndex& dbindex = const_cast<SliceIndex&>(index);
    dbindex.IOtype(iotype);
    dbindex.mIOFlag = ioflag;
}

void xRedisClient::SetErrMessage(const SliceIndex& index, const char* fmt,
    ...)
{
    char szBuf[512] = { 0 };
    va_list va;
    va_start(va, fmt);
    vsnprintf(szBuf, sizeof(szBuf), fmt, va);
    va_end(va);
    SetErrString(index, szBuf, ::strlen(szBuf));
}

rReply* xRedisClient::command(const SliceIndex& index, const char* cmd)
{
    RedisConnection* pRedisConn = mRedisPool->GetConnection(index.mType, index.mIndex, index.mIOtype);
    if (NULL == pRedisConn) {
        SetErrMessage(index, GET_CONNECT_ERROR " %s\r\n", cmd);
        //SetErrString(index, GET_CONNECT_ERROR, ::strlen(GET_CONNECT_ERROR));
        return NULL;
    }
    rReply* reply = static_cast<rReply*>(redisCommand(pRedisConn->GetCtx(), cmd));

    mRedisPool->FreeConnection(pRedisConn);
    return reply;
}

bool xRedisClient::command_bool(const SliceIndex& index, const char* cmd, ...)
{
    bool bRet = false;
    RedisConnection* pRedisConn = mRedisPool->GetConnection(index.mType, index.mIndex, index.mIOtype);
    if (NULL == pRedisConn) {
        SetErrMessage(index, GET_CONNECT_ERROR " %s\r\n", cmd);
        //SetErrString(index, GET_CONNECT_ERROR, ::strlen(GET_CONNECT_ERROR));
        return false;
    }

    va_list args;
    va_start(args, cmd);
    redisReply* reply = static_cast<redisReply*>(redisvCommand(pRedisConn->GetCtx(), cmd, args));
    va_end(args);

    if (RedisPool::CheckReply(reply)) {
        if (REDIS_REPLY_STATUS == reply->type) {
            bRet = true;
        } else {
            bRet = (reply->integer == 1) ? true : false;
        }
    } else {
        SetErrInfo(index, reply);
    }

    RedisPool::FreeReply(reply);
    mRedisPool->FreeConnection(pRedisConn);

    return bRet;
}

bool xRedisClient::command_status(const SliceIndex& index, const char* cmd,
    ...)
{
    bool bRet = false;
    RedisConnection* pRedisConn = mRedisPool->GetConnection(index.mType, index.mIndex, index.mIOtype);
    if (NULL == pRedisConn) {
        SetErrMessage(index, GET_CONNECT_ERROR " %s\r\n", cmd);
        //SetErrString(index, GET_CONNECT_ERROR, ::strlen(GET_CONNECT_ERROR));
        return false;
    }

    va_list args;
    va_start(args, cmd);
    redisReply* reply = static_cast<redisReply*>(redisvCommand(pRedisConn->GetCtx(), cmd, args));
    va_end(args);

    if (RedisPool::CheckReply(reply)) {
        // Assume good reply until further inspection
        bRet = true;
        if (REDIS_REPLY_STRING == reply->type) {
            if (!reply->len || !reply->str || strcasecmp(reply->str, "OK") != 0) {
                bRet = false;
            }
        }
    } else {
        SetErrInfo(index, reply);
    }

    RedisPool::FreeReply(reply);
    mRedisPool->FreeConnection(pRedisConn);

    return bRet;
}

bool xRedisClient::command_integer(const SliceIndex& index, int64_t& retval,
    const char* cmd, ...)
{
    bool bRet = false;
    RedisConnection* pRedisConn = mRedisPool->GetConnection(index.mType, index.mIndex, index.mIOtype);
    if (NULL == pRedisConn) {
        SetErrMessage(index, GET_CONNECT_ERROR " %s\r\n", cmd);
        //SetErrString(index, GET_CONNECT_ERROR, ::strlen(GET_CONNECT_ERROR));
        return false;
    }

    va_list args;
    va_start(args, cmd);
    redisReply* reply = static_cast<redisReply*>(redisvCommand(pRedisConn->GetCtx(), cmd, args));
    va_end(args);
    if (RedisPool::CheckReply(reply)) {
        retval = reply->integer;
        bRet = true;
    } else {
        SetErrInfo(index, reply);
    }

    RedisPool::FreeReply(reply);
    mRedisPool->FreeConnection(pRedisConn);

    return bRet;
}

bool xRedisClient::command_string(const SliceIndex& index, std::string& data,
    const char* cmd, ...)
{
    bool bRet = false;
    RedisConnection* pRedisConn = mRedisPool->GetConnection(index.mType, index.mIndex, index.mIOtype);
    if (NULL == pRedisConn) {
        SetErrMessage(index, GET_CONNECT_ERROR " %s\r\n", cmd);
        //SetErrString(index, GET_CONNECT_ERROR, ::strlen(GET_CONNECT_ERROR));
        return false;
    }

    va_list args;
    va_start(args, cmd);
    redisReply* reply = static_cast<redisReply*>(redisvCommand(pRedisConn->GetCtx(), cmd, args));
    va_end(args);
    if (RedisPool::CheckReply(reply)) {
        data.assign(reply->str, reply->len);
        bRet = true;
    } else {
        SetErrInfo(index, reply);
    }

    RedisPool::FreeReply(reply);
    mRedisPool->FreeConnection(pRedisConn);

    return bRet;
}

bool xRedisClient::command_list(const SliceIndex& index, VALUES& vValue,
    const char* cmd, ...)
{
    bool bRet = false;
    RedisConnection* pRedisConn = mRedisPool->GetConnection(index.mType, index.mIndex, index.mIOtype);
    if (NULL == pRedisConn) {
        SetErrMessage(index, GET_CONNECT_ERROR " %s\r\n", cmd);
        //SetErrString(index, GET_CONNECT_ERROR, ::strlen(GET_CONNECT_ERROR));
        return false;
    }

    va_list args;
    va_start(args, cmd);
    redisReply* reply = static_cast<redisReply*>(redisvCommand(pRedisConn->GetCtx(), cmd, args));
    va_end(args);
    if (RedisPool::CheckReply(reply)) {
        for (size_t i = 0; i < reply->elements; i++) {
            vValue.push_back(
                std::string(reply->element[i]->str, reply->element[i]->len));
        }
        bRet = true;
    } else {
        SetErrInfo(index, reply);
    }

    RedisPool::FreeReply(reply);
    mRedisPool->FreeConnection(pRedisConn);

    return bRet;
}

bool xRedisClient::command_array(const SliceIndex& index, ArrayReply& array,
    const char* cmd, ...)
{
    bool bRet = false;
    RedisConnection* pRedisConn = mRedisPool->GetConnection(index.mType, index.mIndex, index.mIOtype);
    if (NULL == pRedisConn) {
        SetErrMessage(index, GET_CONNECT_ERROR " %s\r\n", cmd);
        //SetErrString(index, GET_CONNECT_ERROR, ::strlen(GET_CONNECT_ERROR));
        return false;
    }

    va_list args;
    va_start(args, cmd);
    redisReply* reply = static_cast<redisReply*>(redisvCommand(pRedisConn->GetCtx(), cmd, args));
    va_end(args);
    if (RedisPool::CheckReply(reply)) {
        for (size_t i = 0; i < reply->elements; i++) {
            DataItem item;
            item.type = reply->element[i]->type;
            item.str.assign(reply->element[i]->str, reply->element[i]->len);
            array.push_back(item);
        }
        bRet = true;
    } else {
        SetErrInfo(index, reply);
    }

    RedisPool::FreeReply(reply);
    mRedisPool->FreeConnection(pRedisConn);
    return bRet;
}

bool xRedisClient::commandargv_array_ex(const SliceIndex& index,
    const VDATA& vDataIn,
    xRedisContext& ctx)
{
    bool bRet = false;
    RedisConnection* pRedisConn = mRedisPool->GetConnection(index.mType, index.mIndex, index.mIOtype);
    if (NULL == pRedisConn) {
        SetErrMessage(index, GET_CONNECT_ERROR " type:%u index:%u IOtype:%u \r\n",
            index.mType, index.mIndex, index.mIOtype);
        //SetErrString(index, GET_CONNECT_ERROR, ::strlen(GET_CONNECT_ERROR));
        return false;
    }

    std::vector<const char*> argv(vDataIn.size());
    std::vector<size_t> argvlen(vDataIn.size());
    uint32_t j = 0;
    for (VDATA::const_iterator i = vDataIn.begin(); i != vDataIn.end();
         ++i, ++j) {
        argv[j] = i->c_str(), argvlen[j] = i->size();
    }

    redisReply* reply = static_cast<redisReply*>(redisCommandArgv(
        pRedisConn->GetCtx(), argv.size(), &(argv[0]), &(argvlen[0])));
    if (RedisPool::CheckReply(reply)) {
        bRet = true;
    } else {
        SetErrInfo(index, reply);
    }

    RedisPool::FreeReply(reply);
    ctx.conn = pRedisConn;
    return bRet;
}

int32_t xRedisClient::GetReply(xRedisContext* ctx, ReplyData& vData)
{
    redisReply *reply = NULL;
    RedisConnection* pRedisConn = static_cast<RedisConnection*>(ctx->conn);

    int32_t ret = redisGetReply(pRedisConn->GetCtx(), (void**)&reply);
    if (0 == ret) {
        for (size_t i = 0; i < reply->elements; i++) {
            DataItem item;
            item.type = reply->element[i]->type;
            item.str.assign(reply->element[i]->str, reply->element[i]->len);
            vData.push_back(item);
        }
    }
    RedisPool::FreeReply(reply);
    return ret;
}

bool xRedisClient::GetxRedisContext(const SliceIndex& index,
    xRedisContext* ctx)
{
    RedisConnection* pRedisConn = mRedisPool->GetConnection(index.mType, index.mIndex, index.mIOtype);
    if (NULL == pRedisConn) {
        return false;
    }
    ctx->conn = pRedisConn;
    return true;
}

void xRedisClient::FreexRedisContext(xRedisContext* ctx)
{
    RedisConnection* pRedisConn = static_cast<RedisConnection*>(ctx->conn);
    redisReply* reply = static_cast<redisReply*>(
        redisCommand(pRedisConn->GetCtx(), "unsubscribe"));
    RedisPool::FreeReply(reply);
    mRedisPool->FreeConnection((RedisConnection*)ctx->conn);
}

bool xRedisClient::commandargv_bool(const SliceIndex& index,
    const VDATA& vData)
{
    bool bRet = false;
    RedisConnection* pRedisConn = mRedisPool->GetConnection(index.mType, index.mIndex, index.mIOtype);
    if (NULL == pRedisConn) {
        SetErrMessage(index, GET_CONNECT_ERROR " type:%u index:%u IOtype:%u \r\n",
            index.mType, index.mIndex, index.mIOtype);
        //SetErrString(index, GET_CONNECT_ERROR, ::strlen(GET_CONNECT_ERROR));
        return bRet;
    }

    std::vector<const char*> argv(vData.size());
    std::vector<size_t> argvlen(vData.size());
    uint32_t j = 0;
    for (VDATA::const_iterator i = vData.begin(); i != vData.end(); ++i, ++j) {
        argv[j] = i->c_str(), argvlen[j] = i->size();
    }

    redisReply* reply = static_cast<redisReply*>(redisCommandArgv(
        pRedisConn->GetCtx(), argv.size(), &(argv[0]), &(argvlen[0])));
    if (RedisPool::CheckReply(reply)) {
        bRet = (reply->integer == 1) ? true : false;
    } else {
        SetErrInfo(index, reply);
    }

    RedisPool::FreeReply(reply);
    mRedisPool->FreeConnection(pRedisConn);

    return bRet;
}

bool xRedisClient::commandargv_status(const SliceIndex& index,
    const VDATA& vData)
{
    bool bRet = false;
    RedisConnection* pRedisConn = mRedisPool->GetConnection(index.mType, index.mIndex, index.mIOtype);
    if (NULL == pRedisConn) {
        SetErrMessage(index, GET_CONNECT_ERROR " type:%u index:%u IOtype:%u \r\n",
            index.mType, index.mIndex, index.mIOtype);
        //SetErrString(index, GET_CONNECT_ERROR, ::strlen(GET_CONNECT_ERROR));
        return bRet;
    }

    std::vector<const char*> argv(vData.size());
    std::vector<size_t> argvlen(vData.size());
    uint32_t j = 0;
    for (VDATA::const_iterator i = vData.begin(); i != vData.end(); ++i, ++j) {
        argv[j] = i->c_str(), argvlen[j] = i->size();
    }

    redisReply* reply = static_cast<redisReply*>(redisCommandArgv(
        pRedisConn->GetCtx(), argv.size(), &(argv[0]), &(argvlen[0])));
    if (RedisPool::CheckReply(reply)) {
        // Assume good reply until further inspection
        bRet = true;

        if (REDIS_REPLY_STRING == reply->type) {
            if (!reply->len || !reply->str || strcasecmp(reply->str, "OK") != 0) {
                bRet = false;
            }
        }
    } else {
        SetErrInfo(index, reply);
    }

    RedisPool::FreeReply(reply);
    mRedisPool->FreeConnection(pRedisConn);

    return bRet;
}

bool xRedisClient::commandargv_array(const SliceIndex& index,
    const VDATA& vDataIn, ArrayReply& array)
{
    bool bRet = false;
    RedisConnection* pRedisConn = mRedisPool->GetConnection(index.mType, index.mIndex, index.mIOtype);
    if (NULL == pRedisConn) {
        //SetErrString(index, GET_CONNECT_ERROR, ::strlen(GET_CONNECT_ERROR));
        SetErrMessage(index, GET_CONNECT_ERROR " type:%u index:%u IOtype:%u \r\n",
            index.mType, index.mIndex, index.mIOtype);
        return false;
    }

    std::vector<const char*> argv(vDataIn.size());
    std::vector<size_t> argvlen(vDataIn.size());
    uint32_t j = 0;
    for (VDATA::const_iterator i = vDataIn.begin(); i != vDataIn.end();
         ++i, ++j) {
        argv[j] = i->c_str(), argvlen[j] = i->size();
    }

    redisReply* reply = static_cast<redisReply*>(redisCommandArgv(
        pRedisConn->GetCtx(), argv.size(), &(argv[0]), &(argvlen[0])));
    if (RedisPool::CheckReply(reply)) {
        for (size_t i = 0; i < reply->elements; i++) {
            DataItem item;
            item.type = reply->element[i]->type;
            item.str.assign(reply->element[i]->str, reply->element[i]->len);
            array.push_back(item);
        }
        bRet = true;
    } else {
        SetErrInfo(index, reply);
    }

    RedisPool::FreeReply(reply);
    mRedisPool->FreeConnection(pRedisConn);
    return bRet;
}

bool xRedisClient::commandargv_array(const SliceIndex& index,
    const VDATA& vDataIn, VALUES& array)
{
    bool bRet = false;
    RedisConnection* pRedisConn = mRedisPool->GetConnection(index.mType, index.mIndex, index.mIOtype);
    if (NULL == pRedisConn) {
        SetErrMessage(index, GET_CONNECT_ERROR " type:%u index:%u IOtype:%u \r\n",
            index.mType, index.mIndex, index.mIOtype);
        //SetErrString(index, GET_CONNECT_ERROR, ::strlen(GET_CONNECT_ERROR));
        return false;
    }

    std::vector<const char*> argv(vDataIn.size());
    std::vector<size_t> argvlen(vDataIn.size());
    uint32_t j = 0;
    for (VDATA::const_iterator i = vDataIn.begin(); i != vDataIn.end();
         ++i, ++j) {
        argv[j] = i->c_str(), argvlen[j] = i->size();
    }

    redisReply* reply = static_cast<redisReply*>(redisCommandArgv(
        pRedisConn->GetCtx(), argv.size(), &(argv[0]), &(argvlen[0])));
    if (RedisPool::CheckReply(reply)) {
        for (size_t i = 0; i < reply->elements; i++) {
            std::string str(reply->element[i]->str, reply->element[i]->len);
            array.push_back(str);
        }
        bRet = true;
    } else {
        SetErrInfo(index, reply);
    }

    RedisPool::FreeReply(reply);
    mRedisPool->FreeConnection(pRedisConn);
    return bRet;
}

bool xRedisClient::commandargv_integer(const SliceIndex& index,
    const VDATA& vDataIn, int64_t& retval)
{
    bool bRet = false;
    RedisConnection* pRedisConn = mRedisPool->GetConnection(index.mType, index.mIndex, index.mIOtype);
    if (NULL == pRedisConn) {
        SetErrMessage(index, GET_CONNECT_ERROR " type:%u index:%u IOtype:%u \r\n",
            index.mType, index.mIndex, index.mIOtype);
        //SetErrString(index, GET_CONNECT_ERROR, ::strlen(GET_CONNECT_ERROR));
        return false;
    }

    std::vector<const char*> argv(vDataIn.size());
    std::vector<size_t> argvlen(vDataIn.size());
    uint32_t j = 0;
    for (VDATA::const_iterator iter = vDataIn.begin(); iter != vDataIn.end();
         ++iter, ++j) {
        argv[j] = iter->c_str(), argvlen[j] = iter->size();
    }

    redisReply* reply = static_cast<redisReply*>(redisCommandArgv(
        pRedisConn->GetCtx(), argv.size(), &(argv[0]), &(argvlen[0])));
    if (RedisPool::CheckReply(reply)) {
        retval = reply->integer;
        bRet = true;
    } else {
        SetErrInfo(index, reply);
    }

    RedisPool::FreeReply(reply);
    mRedisPool->FreeConnection(pRedisConn);
    return bRet;
}

bool xRedisClient::ScanFun(const char* cmd, const SliceIndex& index,
    const std::string* key, int64_t& cursor,
    const char* pattern, uint32_t count,
    ArrayReply& array, xRedisContext& ctx)
{
    SETDEFAULTIOTYPE(MASTER);
    VDATA vCmdData;
    vCmdData.push_back(cmd);
    if (NULL != key) {
        vCmdData.push_back(*key);
    }

    vCmdData.push_back(toString(cursor));

    if (NULL != pattern) {
        vCmdData.push_back("MATCH");
        vCmdData.push_back(pattern);
    }

    if (0 != count) {
        vCmdData.push_back("COUNT");
        vCmdData.push_back(toString(count));
    }

    bool bRet = false;
    RedisConnection* pRedisConn = static_cast<RedisConnection*>(ctx.conn);
    if (NULL == pRedisConn) {
        SetErrMessage(index, GET_CONNECT_ERROR " key:%s type:%u index:%u IOtype:%u \r\n", key->c_str(),
            index.mType, index.mIndex, index.mIOtype);
        //SetErrString(index, GET_CONNECT_ERROR, ::strlen(GET_CONNECT_ERROR));
        return false;
    }

    std::vector<const char*> argv(vCmdData.size());
    std::vector<size_t> argvlen(vCmdData.size());
    uint32_t j = 0;
    for (VDATA::const_iterator i = vCmdData.begin(); i != vCmdData.end();
         ++i, ++j) {
        argv[j] = i->c_str(), argvlen[j] = i->size();
    }

    redisReply* reply = static_cast<redisReply*>(redisCommandArgv(
        pRedisConn->GetCtx(), argv.size(), &(argv[0]), &(argvlen[0])));
    if (RedisPool::CheckReply(reply)) {
        if (0 == reply->elements) {
            cursor = 0;
        } else {
            cursor = atoi(reply->element[0]->str);
            redisReply** replyData = reply->element[1]->element;
            for (size_t i = 0; i < reply->element[1]->elements; i++) {
                DataItem item;
                item.type = replyData[i]->type;
                item.str.assign(replyData[i]->str, replyData[i]->len);
                array.push_back(item);
            }
        }
        bRet = true;
    } else {
        SetErrInfo(index, reply);
    }
    RedisPool::FreeReply(reply);
    return bRet;
}
