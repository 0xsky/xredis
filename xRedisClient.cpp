
#include "xRedisClient.h"
#include <sstream>

bool xRedisClient::Init(unsigned int maxtype) {
    if(NULL==mRedisPool) {
        mRedisPool = new RedisPool;
        mRedisPool->Init(maxtype);
        return mRedisPool!=NULL;
    }
    return false;
}

void xRedisClient::release() {
    if (NULL!=mRedisPool) {
        mRedisPool->Release();
        delete mRedisPool;
        mRedisPool = NULL;
    }
}

void xRedisClient::KeepAlive() {
    if (NULL!=mRedisPool) {
        mRedisPool->KeepAlive();
    }
}

RedisDBIdx xRedisClient::GetDBIndex(const char *key,  HASHFUN fun, const unsigned int type) {
    RedisDBIdx dbi;
    dbi.type  = type;
    unsigned int hashbase = mRedisPool->getHashBase(type);
    if (hashbase>0&& hashbase<MAX_REDIS_DB_HASHBASE) {
        dbi.isvalid = true;
        dbi.index   = fun(key)%hashbase;
    }
    return dbi;
}

RedisDBIdx xRedisClient::GetDBIndex(const int64_t uid, const unsigned int type) {
    RedisDBIdx dbi;
    dbi.type  = type;
    unsigned int hashbase = mRedisPool->getHashBase(type);
    if (hashbase>0&& hashbase<MAX_REDIS_DB_HASHBASE) {
        dbi.isvalid = true;
        dbi.index   = uid%hashbase;
    }
    return dbi;
}

inline RedisPool *xRedisClient::GetRedisPool() { 
    return mRedisPool;
}

bool xRedisClient::ConnectRedisCache( const RedisNode *redisnodelist, unsigned int hashbase, unsigned int cachetype) {
    if (NULL==mRedisPool) {
        return false;
    }
    mRedisPool->setHashBase(cachetype, hashbase);
    for (unsigned int n = 0; n<hashbase; n++) {
        const RedisNode *pNode = &redisnodelist[n];
        if (NULL==pNode) {
            return false;
        }

        mRedisPool->ConnectRedisDB(cachetype, pNode->dbindex, pNode->host, pNode->port, 
            pNode->passwd, pNode->poolsize, pNode->timeout);
    }

    return true;
}


bool xRedisClient::command_bool(const RedisDBIdx& dbi, const char *cmd, ...) {
    bool bRet = false;
    RedisConn *pRedisConn = mRedisPool->GetConnection(dbi.type, dbi.index);
    if (NULL==pRedisConn) {
        return false;
    }

    va_list args;
    va_start(args, cmd);
    redisReply *reply = static_cast<redisReply *>(redisvCommand(pRedisConn->getCtx(), cmd, args));
    va_end(args);

    if (RedisPool::CheckReply(reply)) {
        bRet = (reply->integer)==1?true:false;
    } else {
        RedisDBIdx &dbindex = const_cast<RedisDBIdx&>(dbi);
        dbindex.SetErrInfo(reply->str);
    }

    RedisPool::FreeReply(reply);
    mRedisPool->FreeConnection(pRedisConn);

    return bRet;
}

bool xRedisClient::command_status(const RedisDBIdx& dbi, const char* cmd, ...) {
    bool bRet = false;
    RedisConn *pRedisConn = mRedisPool->GetConnection(dbi.type, dbi.index);
    if (NULL==pRedisConn) {
        return false;
    }

    va_list args;
    va_start(args, cmd);
    redisReply *reply = static_cast<redisReply *>(redisvCommand(pRedisConn->getCtx(), cmd, args));
    va_end(args);

    if (RedisPool::CheckReply(reply)) {
        bRet = true;
    } else {
        RedisDBIdx &dbindex = const_cast<RedisDBIdx&>(dbi);
        dbindex.SetErrInfo(reply->str);
    }

    RedisPool::FreeReply(reply);
    mRedisPool->FreeConnection(pRedisConn);

    return bRet;
}

bool xRedisClient::command_integer(const RedisDBIdx& dbi, int64_t &retval, const char* cmd, ...) {
    bool bRet = false;
    RedisConn *pRedisConn = mRedisPool->GetConnection(dbi.type, dbi.index);
    if (NULL==pRedisConn) {
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
        RedisDBIdx &dbindex = const_cast<RedisDBIdx&>(dbi);
        dbindex.SetErrInfo(reply->str);
    }

    RedisPool::FreeReply(reply);
    mRedisPool->FreeConnection(pRedisConn);

    return bRet;
}

bool xRedisClient::command_string(const RedisDBIdx& dbi, string &data, const char* cmd, ...) {
    bool bRet = false;
    RedisConn *pRedisConn = mRedisPool->GetConnection(dbi.type, dbi.index);
    if (NULL==pRedisConn) {
        return false;
    }

    va_list args;
    va_start(args, cmd);
    redisReply *reply = static_cast<redisReply *>(redisvCommand(pRedisConn->getCtx(), cmd, args));
    va_end(args);
    if (RedisPool::CheckReply(reply)) {
        data = reply->str;
        bRet = true;
    } else {
        RedisDBIdx &dbindex = const_cast<RedisDBIdx&>(dbi);
        dbindex.SetErrInfo(reply->str);
    }

    RedisPool::FreeReply(reply);
    mRedisPool->FreeConnection(pRedisConn);

    return bRet;
}

bool xRedisClient::command_list(const RedisDBIdx& dbi, VALUES &vValue, const char* cmd, ...) {
    bool bRet = false;
    RedisConn *pRedisConn = mRedisPool->GetConnection(dbi.type, dbi.index);
    if (NULL==pRedisConn) {
        return false;
    }

    va_list args;
    va_start(args, cmd);
    redisReply *reply = static_cast<redisReply *>(redisvCommand(pRedisConn->getCtx(), cmd, args));
    va_end(args);
    if (RedisPool::CheckReply(reply)) {
        for (unsigned int i =0; i<reply->elements; i++) {
            vValue.push_back(reply->element[i]->str);
        }
        bRet  = true;
    } else {
        RedisDBIdx &dbindex = const_cast<RedisDBIdx&>(dbi);
        dbindex.SetErrInfo(reply->str);
    }

    RedisPool::FreeReply(reply);
    mRedisPool->FreeConnection(pRedisConn);

    return bRet;
}

bool xRedisClient::command_array(const RedisDBIdx& dbi,  ArrayReply& array,  const char* cmd, ...){
    bool bRet = false;
    RedisConn *pRedisConn = mRedisPool->GetConnection(dbi.type, dbi.index);
    if (NULL==pRedisConn) {
        return false;
    }

    va_list args;
    va_start(args, cmd);
    redisReply *reply = static_cast<redisReply *>(redisvCommand(pRedisConn->getCtx(), cmd, args));
    va_end(args);
    if (RedisPool::CheckReply(reply)) {
        for (unsigned int i =0; i<reply->elements; i++) {
            DataItem item;
            item.type = reply->element[i]->type;
            item.str  = reply->element[i]->str;
            array.push_back(item);
        }
        bRet  = true;
    } else {
        RedisDBIdx &dbindex = const_cast<RedisDBIdx&>(dbi);
        dbindex.SetErrInfo(reply->str);
    }

    RedisPool::FreeReply(reply);
    mRedisPool->FreeConnection(pRedisConn);
    return bRet;
}

bool xRedisClient::commandargv_bool(const RedisDBIdx& dbi, const VDATA& vData) {
    bool bRet = false;
    RedisConn *pRedisConn = mRedisPool->GetConnection(dbi.type, dbi.index);
    if (NULL==pRedisConn) {
        return bRet;
    }

    vector<const char *> argv( vData.size() );
    vector<size_t> argvlen( vData.size() );
    unsigned int j = 0;
    for ( vector<string>::const_iterator i = vData.begin(); i != vData.end(); ++i, ++j ) {
        argv[j] = i->c_str(), argvlen[j] = i->size();
    }

    redisReply *reply = static_cast<redisReply *>(redisCommandArgv(pRedisConn->getCtx(), argv.size(), &(argv[0]), &(argvlen[0])));
    if (RedisPool::CheckReply(reply)) {
        bRet = (reply->integer)==1?true:false;
    }

    RedisPool::FreeReply(reply);
    mRedisPool->FreeConnection(pRedisConn);

    return bRet;
}

bool xRedisClient::commandargv_status(const RedisDBIdx& dbi, const VDATA& vData) {
    bool bRet = false;
    RedisConn *pRedisConn = mRedisPool->GetConnection(dbi.type, dbi.index);
    if (NULL==pRedisConn) {
        return bRet;
    }

    vector<const char *> argv( vData.size() );
    vector<size_t> argvlen( vData.size() );
    unsigned int j = 0;
    for ( vector<string>::const_iterator i = vData.begin(); i != vData.end(); ++i, ++j ) {
        argv[j] = i->c_str(), argvlen[j] = i->size();
    }

    redisReply *reply = static_cast<redisReply *>(redisCommandArgv(pRedisConn->getCtx(), argv.size(), &(argv[0]), &(argvlen[0])));
    if (RedisPool::CheckReply(reply)) {
        bRet = true;
    }

    RedisPool::FreeReply(reply);
    mRedisPool->FreeConnection(pRedisConn);

    return bRet;
}

bool xRedisClient::commandargv_array(const RedisDBIdx& dbi, const VDATA& vDataIn, ArrayReply& array){
    bool bRet = false;
    RedisConn *pRedisConn = mRedisPool->GetConnection(dbi.type, dbi.index);
    if (NULL==pRedisConn) {
        return false;
    }

    vector<const char*> argv( vDataIn.size() );
    vector<size_t> argvlen( vDataIn.size() );
    unsigned int j = 0;
    for ( vector<string>::const_iterator i = vDataIn.begin(); i != vDataIn.end(); ++i, ++j ) {
        argv[j] = i->c_str(), argvlen[j] = i->size();
    }

    redisReply *reply = static_cast<redisReply *>(redisCommandArgv(pRedisConn->getCtx(), argv.size(), &(argv[0]), &(argvlen[0])));
    if (RedisPool::CheckReply(reply)) {
        for (unsigned int i =0; i<reply->elements; i++) {
            DataItem item;
            item.type = reply->element[i]->type;
            item.str  = reply->element[i]->str;
            array.push_back(item);
        }
        bRet  = true;
    } else {
        RedisDBIdx &dbindex = const_cast<RedisDBIdx&>(dbi);
        dbindex.SetErrInfo(reply->str);
    }

    RedisPool::FreeReply(reply);
    mRedisPool->FreeConnection(pRedisConn);
    return bRet;
}

bool xRedisClient::commandargv_array(const RedisDBIdx& dbi, const VDATA& vDataIn, VALUES& array){
    bool bRet = false;
    RedisConn *pRedisConn = mRedisPool->GetConnection(dbi.type, dbi.index);
    if (NULL==pRedisConn) {
        return false;
    }

    vector<const char*> argv( vDataIn.size() );
    vector<size_t> argvlen( vDataIn.size() );
    unsigned int j = 0;
    for ( vector<string>::const_iterator i = vDataIn.begin(); i != vDataIn.end(); ++i, ++j ) {
        argv[j] = i->c_str(), argvlen[j] = i->size();
    }

    redisReply *reply = static_cast<redisReply *>(redisCommandArgv(pRedisConn->getCtx(), argv.size(), &(argv[0]), &(argvlen[0])));
    if (RedisPool::CheckReply(reply)) {
        for (unsigned int i =0; i<reply->elements; i++) {
            array.push_back(reply->element[i]->str);
        }
        bRet  = true;
    } else {
        RedisDBIdx &dbindex = const_cast<RedisDBIdx&>(dbi);
        dbindex.SetErrInfo(reply->str);
    }

    RedisPool::FreeReply(reply);
    mRedisPool->FreeConnection(pRedisConn);
    return bRet;
}

bool xRedisClient::commandargv_integer(const RedisDBIdx& dbi, const VDATA& vDataIn, int64_t& retval){
    bool bRet = false;
    RedisConn *pRedisConn = mRedisPool->GetConnection(dbi.type, dbi.index);
    if (NULL==pRedisConn) {
        return false;
    }

    vector<const char*> argv( vDataIn.size() );
    vector<size_t> argvlen( vDataIn.size() );
    unsigned int j = 0;
    for ( vector<string>::const_iterator iter = vDataIn.begin(); iter != vDataIn.end(); ++iter, ++j ) {
        argv[j] = iter->c_str(), argvlen[j] = iter->size();
    }

    redisReply *reply = static_cast<redisReply *>(redisCommandArgv(pRedisConn->getCtx(), argv.size(), &(argv[0]), &(argvlen[0])));
    if (RedisPool::CheckReply(reply)) {
        retval = reply->integer;
        bRet  = true;
    } else {
        RedisDBIdx &dbindex = const_cast<RedisDBIdx&>(dbi);
        dbindex.SetErrInfo(reply->str);
    }

    RedisPool::FreeReply(reply);
    mRedisPool->FreeConnection(pRedisConn);
    return bRet;
}



