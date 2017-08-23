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
#include <stdlib.h>

bool xRedisClient::hdel(const RedisDBIdx& dbi,    const string& key, const string& field, int64_t& count) {
    SETDEFAULTIOTYPE(MASTER);
    return command_integer(dbi, count, "HDEL %s %s", key.c_str(), field.c_str());
}

bool xRedisClient::hdel(const RedisDBIdx& dbi,    const string& key, const KEYS& vfiled, int64_t& count) {
    VDATA vCmdData;
    vCmdData.push_back("HDEL");
    vCmdData.push_back(key);
    addparam(vCmdData, vfiled);
    SETDEFAULTIOTYPE(MASTER);
    return commandargv_integer(dbi, vCmdData, count);
}

bool xRedisClient::hexist(const RedisDBIdx& dbi,   const string& key, const string& field){
    SETDEFAULTIOTYPE(SLAVE);
    return command_bool(dbi,"HEXISTS %s %s", key.c_str(), field.c_str());
}

bool xRedisClient::hget(const RedisDBIdx& dbi,    const string& key, const string& field, string& value) {
    SETDEFAULTIOTYPE(SLAVE);
    return command_string(dbi, value, "HGET %s %s", key.c_str(), field.c_str());
}

bool  xRedisClient::hgetall(const RedisDBIdx& dbi,    const string& key, ArrayReply& array){
    SETDEFAULTIOTYPE(SLAVE);
    return command_array(dbi, array, "HGETALL %s", key.c_str());
}

bool xRedisClient::hincrby(const RedisDBIdx& dbi,  const string& key, const string& field, int64_t increment, int64_t& num ) {
    SETDEFAULTIOTYPE(MASTER);
    return command_integer(dbi, num, "HINCRBY %s %s %lld", key.c_str(),field.c_str(), increment);
}

bool xRedisClient::hincrbyfloat(const RedisDBIdx& dbi,  const string& key, const string& field, float increment, float& value ) {
    SETDEFAULTIOTYPE(MASTER);
    bool bRet = false;
    RedisConn *pRedisConn = mRedisPool->GetConnection(dbi.mType, dbi.mIndex);
    if (NULL==pRedisConn) {
        return false;
    }

    redisReply *reply = static_cast<redisReply *>(redisCommand(pRedisConn->getCtx(), "HINCRBYFLOAT %s %s %f", key.c_str(), field.c_str(), increment));
    if (RedisPool::CheckReply(reply)) {
        value = atof(reply->str);
        bRet = true;
    }

    RedisPool::FreeReply(reply);
    mRedisPool->FreeConnection(pRedisConn);
    return bRet;
}

bool xRedisClient::hkeys(const RedisDBIdx& dbi,  const string& key, KEYS& keys){
    SETDEFAULTIOTYPE(SLAVE);
    return command_list(dbi, keys, "HKEYS %s", key.c_str());
}

bool xRedisClient::hlen(const RedisDBIdx& dbi,  const string& key, int64_t& count){
    SETDEFAULTIOTYPE(SLAVE);
    return command_integer(dbi, count, "HLEN %s", key.c_str());
}

bool xRedisClient::hmget(const RedisDBIdx& dbi,    const string& key, const KEYS& field, ArrayReply& array){
    VDATA vCmdData;
    vCmdData.push_back("HMGET");
    vCmdData.push_back(key);
    addparam(vCmdData, field);
    SETDEFAULTIOTYPE(SLAVE);
    return commandargv_array(dbi, vCmdData, array);
}

bool xRedisClient::hmset(const RedisDBIdx& dbi,    const string& key, const VDATA& vData){
    VDATA vCmdData;
    vCmdData.push_back("HMSET");
    vCmdData.push_back(key);
    addparam(vCmdData, vData);
    SETDEFAULTIOTYPE(MASTER);
    return commandargv_status(dbi, vCmdData);
}

bool xRedisClient::hscan(const RedisDBIdx& dbi, const std::string& key, int64_t &cursor, const char *pattern, uint32_t count, ArrayReply& array, xRedisContext& ctx)
{
    return ScanFun("HSCAN", dbi, &key, cursor, pattern, count, array, ctx);
}

bool xRedisClient::hset(const RedisDBIdx& dbi,    const string& key, const string& field, const string& value, int64_t& retval){
    SETDEFAULTIOTYPE(MASTER);
    VDATA vCmdData;
    vCmdData.push_back("HSET");
    vCmdData.push_back(key);
    vCmdData.push_back(field);
    vCmdData.push_back(value);
    return commandargv_integer(dbi, vCmdData, retval);
    //return command_integer(dbi, retval, "HSET %s %s %s", key.c_str(), field.c_str(), value.c_str());
}

bool xRedisClient::hsetnx(const RedisDBIdx& dbi,    const string& key, const string& field, const string& value){
    SETDEFAULTIOTYPE(MASTER);
    return command_bool(dbi, "HSETNX %s %s %s", key.c_str(), field.c_str(), value.c_str());
}

bool xRedisClient::hvals(const RedisDBIdx& dbi,  const string& key, VALUES& values) {
    SETDEFAULTIOTYPE(SLAVE);
    return command_list(dbi, values, "HVALS %s", key.c_str());
}


