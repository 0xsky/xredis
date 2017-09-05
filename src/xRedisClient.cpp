/*
 * ----------------------------------------------------------------------------
 * Copyright (c) 2013-2014, xSky <guozhw at gmail dot com>
 * All rights reserved.
 * Distributed under GPL license.
 * ----------------------------------------------------------------------------
 */
 
#include "xRedisClient.h"
#include "xRedisPool.h"
#include <sstream>

RedisDBIdx::RedisDBIdx() {
    mType = 0;
    mIndex = 0;
    mStrerr = NULL;
    mClient = NULL;
    mIOtype = MASTER;
    mIOFlag = false;
}

RedisDBIdx::RedisDBIdx(xRedisClient *xredisclient) {
    mType = 0;
    mIndex = 0;
    mStrerr = NULL;
    mClient = xredisclient;
    mIOtype = MASTER;
    mIOFlag = false;
}
RedisDBIdx::~RedisDBIdx() {
    if (NULL != mStrerr){
        delete[] mStrerr;
        mStrerr = NULL;
    }
}

bool RedisDBIdx::CreateDBIndex(const char *key,  HASHFUN fun, const unsigned int type) {
    unsigned int hashbase = mClient->GetRedisPool()->getHashBase(type);
    if ((NULL!=fun) && (hashbase>0)) {
        mIndex = fun(key)%hashbase;
        mType  = type;
        return true;
    }
    return false;
}

bool RedisDBIdx::CreateDBIndex(const int64_t id, const unsigned int type) {
    unsigned int hashbase = mClient->GetRedisPool()->getHashBase(type);
    if (hashbase>0) {
        mType  = type;
        mIndex = id%hashbase;
        return true;
    }
    return false;
}

void RedisDBIdx::IOtype(unsigned int type) {
    mIOtype = type;
}

void RedisDBIdx::SetIOMaster()
{
    mIOtype = MASTER;
    mIOFlag = true;
}

bool RedisDBIdx::SetErrInfo(const char *info, int len) {
    if (NULL == info) {
        return false;
    }
    if (NULL == mStrerr){
        mStrerr = new char[len + 1];
    }
    if (NULL != mStrerr) {
        strncpy(mStrerr, info, len);
        mStrerr[len] = '\0';
        return true;
    }
    return false;
}

xRedisClient::xRedisClient()
{
    mRedisPool = NULL;
}


xRedisClient::~xRedisClient()
{
    Release();
}

bool xRedisClient::Init(unsigned int maxtype) {
    if(NULL==mRedisPool) {
        mRedisPool = new RedisPool;
        if (NULL==mRedisPool) {
            return false;
        }
        
        return mRedisPool->Init(maxtype);
    }
    return false;
}

void xRedisClient::Release() {
    if (NULL!=mRedisPool) {
        mRedisPool->Release();
        delete mRedisPool;
        mRedisPool = NULL;
    }
}

void xRedisClient::Keepalive() {
    if (NULL!=mRedisPool) {
        mRedisPool->Keepalive();
    }
}

inline RedisPool *xRedisClient::GetRedisPool() { 
    return mRedisPool;
}

void xRedisClient::FreeReply(const rReply* reply)
{
    RedisPool::FreeReply((redisReply*)reply);
}

bool xRedisClient::ConnectRedisCache(const RedisNode *redisnodelist, unsigned int nodecount, unsigned int hashbase, unsigned int cachetype) {
    if (NULL==mRedisPool) {
        return false;
    }
    
    if (!mRedisPool->setHashBase(cachetype, hashbase)) {
        return false;
    }
    
    for (unsigned int n = 0; n<nodecount; n++) {
        const RedisNode *pNode = &redisnodelist[n];
        if (NULL==pNode) {
            return false;
        }

        bool bRet = mRedisPool->ConnectRedisDB(cachetype, pNode->dbindex, pNode->host, pNode->port, 
            pNode->passwd, pNode->poolsize, pNode->timeout, pNode->role);
        if (!bRet) {
            return false;
        }
    }

    return true;
}


void xRedisClient::SetErrInfo(const RedisDBIdx& dbi, void *p) {
    if (NULL==p){
        SetErrString(dbi, CONNECT_CLOSED_ERROR, ::strlen(CONNECT_CLOSED_ERROR));
    } else {
        redisReply *reply = (redisReply*)p;
        SetErrString(dbi, reply->str, reply->len);
    }
}

void xRedisClient::SetErrString(const RedisDBIdx& dbi, const char *str, int len) {
    RedisDBIdx &dbindex = const_cast<RedisDBIdx&>(dbi);
    dbindex.SetErrInfo(str, len);
}

void xRedisClient::SetIOtype(const RedisDBIdx& dbi, unsigned int iotype, bool ioflag) {
    RedisDBIdx &dbindex = const_cast<RedisDBIdx&>(dbi);
    dbindex.IOtype(iotype);
    dbindex.mIOFlag = ioflag;
}

void xRedisClient::SetErrMessage(const RedisDBIdx& dbi, const char* fmt, ...)
{
    char szBuf[128] = { 0 };
    va_list va;
    va_start(va, fmt);
    vsnprintf(szBuf, sizeof(szBuf), fmt, va);
    va_end(va);
    SetErrString(dbi, szBuf, ::strlen(szBuf));
}

rReply *xRedisClient::command(const RedisDBIdx& dbi, const char* cmd)
{
    RedisConn *pRedisConn = mRedisPool->GetConnection(dbi.mType, dbi.mIndex, dbi.mIOtype);
    if (NULL == pRedisConn) {
        SetErrString(dbi, GET_CONNECT_ERROR, ::strlen(GET_CONNECT_ERROR));
        return NULL;
    }
    rReply *reply = static_cast<rReply *>(redisCommand(pRedisConn->getCtx(), cmd));

    mRedisPool->FreeConnection(pRedisConn);
    return reply;
}

bool xRedisClient::command_bool(const RedisDBIdx& dbi, const char *cmd, ...) {
    bool bRet = false;
    RedisConn *pRedisConn = mRedisPool->GetConnection(dbi.mType, dbi.mIndex, dbi.mIOtype);
    if (NULL == pRedisConn) {
        SetErrString(dbi, GET_CONNECT_ERROR, ::strlen(GET_CONNECT_ERROR));
        return false;
    }

    va_list args;
    va_start(args, cmd);
    redisReply *reply = static_cast<redisReply *>(redisvCommand(pRedisConn->getCtx(), cmd, args));
    va_end(args);

    if (RedisPool::CheckReply(reply)) {
        if (REDIS_REPLY_STATUS==reply->type) {
            bRet = true;
        } else {
            bRet = (reply->integer == 1) ? true : false;
        }
    } else {
        SetErrInfo(dbi, reply);
    }

    RedisPool::FreeReply(reply);
    mRedisPool->FreeConnection(pRedisConn);

    return bRet;
}

bool xRedisClient::command_status(const RedisDBIdx& dbi, const char* cmd, ...) {
    bool bRet = false;
    RedisConn *pRedisConn = mRedisPool->GetConnection(dbi.mType, dbi.mIndex, dbi.mIOtype);
    if (NULL == pRedisConn) {
        SetErrString(dbi, GET_CONNECT_ERROR, ::strlen(GET_CONNECT_ERROR));
        return false;
    }

    va_list args;
    va_start(args, cmd);
    redisReply *reply = static_cast<redisReply *>(redisvCommand(pRedisConn->getCtx(), cmd, args));
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
        SetErrInfo(dbi, reply);
    }
 
    RedisPool::FreeReply(reply);
    mRedisPool->FreeConnection(pRedisConn);

    return bRet;
}

bool xRedisClient::command_integer(const RedisDBIdx& dbi, int64_t &retval, const char* cmd, ...) {
    bool bRet = false;
    RedisConn *pRedisConn = mRedisPool->GetConnection(dbi.mType, dbi.mIndex, dbi.mIOtype);
    if (NULL == pRedisConn) {
        SetErrString(dbi, GET_CONNECT_ERROR, ::strlen(GET_CONNECT_ERROR));
        return false;
    }

    va_list args;
    va_start(args, cmd);
    redisReply *reply = static_cast<redisReply *>(redisvCommand(pRedisConn->getCtx(), cmd, args));
    va_end(args);
    if (RedisPool::CheckReply(reply)) {
        retval = reply->integer;
        bRet = true;
    } else {
        SetErrInfo(dbi, reply);
    }

    RedisPool::FreeReply(reply);
    mRedisPool->FreeConnection(pRedisConn);

    return bRet;
}

bool xRedisClient::command_string(const RedisDBIdx& dbi, string &data, const char* cmd, ...) {
    bool bRet = false;
    RedisConn *pRedisConn = mRedisPool->GetConnection(dbi.mType, dbi.mIndex, dbi.mIOtype);
    if (NULL == pRedisConn) {
        SetErrString(dbi, GET_CONNECT_ERROR, ::strlen(GET_CONNECT_ERROR));
        return false;
    }

    va_list args;
    va_start(args, cmd);
    redisReply *reply = static_cast<redisReply *>(redisvCommand(pRedisConn->getCtx(), cmd, args));
    va_end(args);
    if (RedisPool::CheckReply(reply)) {
        data.assign(reply->str, reply->len);
        bRet = true;
    } else {
        SetErrInfo(dbi, reply);
    }

    RedisPool::FreeReply(reply);
    mRedisPool->FreeConnection(pRedisConn);

    return bRet;
}

bool xRedisClient::command_list(const RedisDBIdx& dbi, VALUES &vValue, const char* cmd, ...) {
    bool bRet = false;
    RedisConn *pRedisConn = mRedisPool->GetConnection(dbi.mType, dbi.mIndex, dbi.mIOtype);
    if (NULL == pRedisConn) {
        SetErrString(dbi, GET_CONNECT_ERROR, ::strlen(GET_CONNECT_ERROR));
        return false;
    }

    va_list args;
    va_start(args, cmd);
    redisReply *reply = static_cast<redisReply *>(redisvCommand(pRedisConn->getCtx(), cmd, args));
    va_end(args);
    if (RedisPool::CheckReply(reply)) {
        for (size_t i = 0; i<reply->elements; i++) {
            vValue.push_back(string(reply->element[i]->str, reply->element[i]->len));
        }
        bRet  = true;
    } else {
        SetErrInfo(dbi, reply);
    }

    RedisPool::FreeReply(reply);
    mRedisPool->FreeConnection(pRedisConn);

    return bRet;
}

bool xRedisClient::command_array(const RedisDBIdx& dbi,  ArrayReply& array,  const char* cmd, ...){
    bool bRet = false;
    RedisConn *pRedisConn = mRedisPool->GetConnection(dbi.mType, dbi.mIndex, dbi.mIOtype);
    if (NULL == pRedisConn) {
        SetErrString(dbi, GET_CONNECT_ERROR, ::strlen(GET_CONNECT_ERROR));
        return false;
    }

    va_list args;
    va_start(args, cmd);
    redisReply *reply = static_cast<redisReply *>(redisvCommand(pRedisConn->getCtx(), cmd, args));
    va_end(args);
    if (RedisPool::CheckReply(reply)) {
        for (size_t i = 0; i<reply->elements; i++) {
            DataItem item;
            item.type = reply->element[i]->type;
            item.str.assign(reply->element[i]->str, reply->element[i]->len);
            array.push_back(item);
        }
        bRet  = true;
    } else {
        SetErrInfo(dbi, reply);
    }

    RedisPool::FreeReply(reply);
    mRedisPool->FreeConnection(pRedisConn);
    return bRet;
}

bool xRedisClient::commandargv_array_ex(const RedisDBIdx& dbi, const VDATA& vDataIn, xRedisContext& ctx){
    bool bRet = false;
    RedisConn *pRedisConn = mRedisPool->GetConnection(dbi.mType, dbi.mIndex, dbi.mIOtype);
    if (NULL == pRedisConn) {
        SetErrString(dbi, GET_CONNECT_ERROR, ::strlen(GET_CONNECT_ERROR));
        return false;
    }

    vector<const char*> argv(vDataIn.size());
    vector<size_t> argvlen(vDataIn.size());
    unsigned int j = 0;
    for (VDATA::const_iterator i = vDataIn.begin(); i != vDataIn.end(); ++i, ++j) {
        argv[j] = i->c_str(), argvlen[j] = i->size();
    }

    redisReply *reply = static_cast<redisReply *>(redisCommandArgv(pRedisConn->getCtx(), argv.size(), &(argv[0]), &(argvlen[0])));
    if (RedisPool::CheckReply(reply)) {
        bRet = true;
    } else {
        SetErrInfo(dbi, reply);
    }

    RedisPool::FreeReply(reply);
    ctx.conn = pRedisConn;
    return bRet;
}

int xRedisClient::GetReply(xRedisContext* ctx, ReplyData& vData)
{
    //vData.clear();
    //ReplyData(vData).swap(vData);
    redisReply *reply;
    RedisConn *pRedisConn = static_cast<RedisConn *>(ctx->conn);
    int ret = redisGetReply(pRedisConn->getCtx(), (void**)&reply);
    if (0==ret) {
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

bool xRedisClient::GetxRedisContext(const RedisDBIdx& dbi, xRedisContext* ctx)
{
    RedisConn *pRedisConn = mRedisPool->GetConnection(dbi.mType, dbi.mIndex, dbi.mIOtype);
    if (NULL == pRedisConn) {
        return false;
    }
    ctx->conn = pRedisConn;
    return true;
}

void xRedisClient::FreexRedisContext(xRedisContext* ctx)
{
    RedisConn *pRedisConn = static_cast<RedisConn *>(ctx->conn);
    redisReply *reply = static_cast<redisReply *>(redisCommand(pRedisConn->getCtx(), "unsubscribe"));
    RedisPool::FreeReply(reply);
    mRedisPool->FreeConnection((RedisConn*)ctx->conn);
}

bool xRedisClient::commandargv_bool(const RedisDBIdx& dbi, const VDATA& vData) {
    bool bRet = false;
    RedisConn *pRedisConn = mRedisPool->GetConnection(dbi.mType, dbi.mIndex, dbi.mIOtype);
    if (NULL == pRedisConn) {
        SetErrString(dbi, GET_CONNECT_ERROR, ::strlen(GET_CONNECT_ERROR));
        return bRet;
    }

    vector<const char *> argv( vData.size() );
    vector<size_t> argvlen( vData.size() );
    unsigned int j = 0;
    for ( VDATA::const_iterator i = vData.begin(); i != vData.end(); ++i, ++j ) {
        argv[j] = i->c_str(), argvlen[j] = i->size();
    }

    redisReply *reply = static_cast<redisReply *>(redisCommandArgv(pRedisConn->getCtx(), argv.size(), &(argv[0]), &(argvlen[0])));
    if (RedisPool::CheckReply(reply)) {
        bRet = (reply->integer==1)?true:false;
    } else {
        SetErrInfo(dbi, reply);
    }

    RedisPool::FreeReply(reply);
    mRedisPool->FreeConnection(pRedisConn);

    return bRet;
}

bool xRedisClient::commandargv_status(const RedisDBIdx& dbi, const VDATA& vData) {
    bool bRet = false;
    RedisConn *pRedisConn = mRedisPool->GetConnection(dbi.mType, dbi.mIndex, dbi.mIOtype);
    if (NULL == pRedisConn) {
        SetErrString(dbi, GET_CONNECT_ERROR, ::strlen(GET_CONNECT_ERROR));
        return bRet;
    }

    vector<const char *> argv( vData.size() );
    vector<size_t> argvlen( vData.size() );
    unsigned int j = 0;
    for ( VDATA::const_iterator i = vData.begin(); i != vData.end(); ++i, ++j ) {
        argv[j] = i->c_str(), argvlen[j] = i->size();
    }

    redisReply *reply = static_cast<redisReply *>(redisCommandArgv(pRedisConn->getCtx(), argv.size(), &(argv[0]), &(argvlen[0])));
    if (RedisPool::CheckReply(reply)) {
        // Assume good reply until further inspection
        bRet = true;
        
        if (REDIS_REPLY_STRING == reply->type) {
            if (!reply->len || !reply->str || strcasecmp(reply->str, "OK") != 0) {
                bRet = false;
            }
        }
    } else {
        SetErrInfo(dbi, reply);
    }
    
    RedisPool::FreeReply(reply);
    mRedisPool->FreeConnection(pRedisConn);

    return bRet;
}

bool xRedisClient::commandargv_array(const RedisDBIdx& dbi, const VDATA& vDataIn, ArrayReply& array){
    bool bRet = false;
    RedisConn *pRedisConn = mRedisPool->GetConnection(dbi.mType, dbi.mIndex, dbi.mIOtype);
    if (NULL == pRedisConn) {
        SetErrString(dbi, GET_CONNECT_ERROR, ::strlen(GET_CONNECT_ERROR));
        return false;
    }

    vector<const char*> argv( vDataIn.size() );
    vector<size_t> argvlen( vDataIn.size() );
    unsigned int j = 0;
    for ( VDATA::const_iterator i = vDataIn.begin(); i != vDataIn.end(); ++i, ++j ) {
        argv[j] = i->c_str(), argvlen[j] = i->size();
    }

    redisReply *reply = static_cast<redisReply *>(redisCommandArgv(pRedisConn->getCtx(), argv.size(), &(argv[0]), &(argvlen[0])));
    if (RedisPool::CheckReply(reply)) {
        for (size_t i = 0; i<reply->elements; i++) {
            DataItem item;
            item.type = reply->element[i]->type;
            item.str.assign(reply->element[i]->str, reply->element[i]->len);
            array.push_back(item);
        }
        bRet  = true;
    } else {
        SetErrInfo(dbi, reply);
    }

    RedisPool::FreeReply(reply);
    mRedisPool->FreeConnection(pRedisConn);
    return bRet;
}

bool xRedisClient::commandargv_array(const RedisDBIdx& dbi, const VDATA& vDataIn, VALUES& array){
    bool bRet = false;
    RedisConn *pRedisConn = mRedisPool->GetConnection(dbi.mType, dbi.mIndex, dbi.mIOtype);
    if (NULL == pRedisConn) {
        SetErrString(dbi, GET_CONNECT_ERROR, ::strlen(GET_CONNECT_ERROR));
        return false;
    }

    vector<const char*> argv( vDataIn.size() );
    vector<size_t> argvlen( vDataIn.size() );
    unsigned int j = 0;
    for ( VDATA::const_iterator i = vDataIn.begin(); i != vDataIn.end(); ++i, ++j ) {
        argv[j] = i->c_str(), argvlen[j] = i->size();
    }

    redisReply *reply = static_cast<redisReply *>(redisCommandArgv(pRedisConn->getCtx(), argv.size(), &(argv[0]), &(argvlen[0])));
    if (RedisPool::CheckReply(reply)) {
        for (size_t i = 0; i<reply->elements; i++) {
            string str(reply->element[i]->str, reply->element[i]->len);
            array.push_back(str);
        }
        bRet  = true;
    } else {
        SetErrInfo(dbi, reply);
    }

    RedisPool::FreeReply(reply);
    mRedisPool->FreeConnection(pRedisConn);
    return bRet;
}

bool xRedisClient::commandargv_integer(const RedisDBIdx& dbi, const VDATA& vDataIn, int64_t& retval){
    bool bRet = false;
    RedisConn *pRedisConn = mRedisPool->GetConnection(dbi.mType, dbi.mIndex, dbi.mIOtype);
    if (NULL == pRedisConn) {
        SetErrString(dbi, GET_CONNECT_ERROR, ::strlen(GET_CONNECT_ERROR));
        return false;
    }

    vector<const char*> argv( vDataIn.size() );
    vector<size_t> argvlen( vDataIn.size() );
    unsigned int j = 0;
    for ( VDATA::const_iterator iter = vDataIn.begin(); iter != vDataIn.end(); ++iter, ++j ) {
        argv[j] = iter->c_str(), argvlen[j] = iter->size();
    }

    redisReply *reply = static_cast<redisReply *>(redisCommandArgv(pRedisConn->getCtx(), argv.size(), &(argv[0]), &(argvlen[0])));
    if (RedisPool::CheckReply(reply)) {
        retval = reply->integer;
        bRet  = true;
    } else {
        SetErrInfo(dbi, reply);
    }

    RedisPool::FreeReply(reply);
    mRedisPool->FreeConnection(pRedisConn);
    return bRet;
}

bool xRedisClient::ScanFun(const char* cmd, const RedisDBIdx& dbi, const std::string *key,
    int64_t &cursor, const char* pattern, uint32_t count, ArrayReply& array, xRedisContext& ctx)
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
    RedisConn *pRedisConn = static_cast<RedisConn *>(ctx.conn);
    if (NULL == pRedisConn) {
        SetErrString(dbi, GET_CONNECT_ERROR, ::strlen(GET_CONNECT_ERROR));
        return false;
    }

    vector<const char*> argv(vCmdData.size());
    vector<size_t> argvlen(vCmdData.size());
    unsigned int j = 0;
    for (VDATA::const_iterator i = vCmdData.begin(); i != vCmdData.end(); ++i, ++j) {
        argv[j] = i->c_str(), argvlen[j] = i->size();
    }

    redisReply *reply = static_cast<redisReply *>(redisCommandArgv(pRedisConn->getCtx(), argv.size(), &(argv[0]), &(argvlen[0])));
    if (RedisPool::CheckReply(reply)) {
        if (0 == reply->elements){
            cursor = 0;
        } else {
            cursor = atoi(reply->element[0]->str);
            redisReply **replyData = reply->element[1]->element;
            for (size_t i = 0; i < reply->element[1]->elements; i++) {
                DataItem item;
                item.type = replyData[i]->type;
                item.str.assign(replyData[i]->str, replyData[i]->len);
                array.push_back(item);
            }
        }
        bRet = true;
    } else {
        SetErrInfo(dbi, reply);
    }
    RedisPool::FreeReply(reply);
    return bRet;
}













