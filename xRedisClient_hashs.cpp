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

bool xRedisClient::hdel(const RedisDBIdx& dbi,    const string& key, const string& filed, int64_t& count) {
    return command_integer(dbi, count, "HDEL %s %s %s", key.c_str(), filed.c_str());
}

bool xRedisClient::hdel(const RedisDBIdx& dbi,    const string& key, const KEYS& vfiled, int64_t& count) {
    VDATA vCmdData;
    vCmdData.push_back("HDEL");
    vCmdData.push_back(key);
    addparam(vCmdData, vfiled);
    return commandargv_integer(dbi, vCmdData, count);
}

bool xRedisClient::hexist(const RedisDBIdx& dbi,   const string& key, const string& filed){
    return command_bool(dbi,"HEXIST %s %s %s", key.c_str(), filed.c_str());
}

bool xRedisClient::hget(const RedisDBIdx& dbi,    const string& key, const string& filed, string& value) {
    return command_string(dbi, value, "HGET %s %s", key.c_str(), filed.c_str());
}

bool  xRedisClient::hgetall(const RedisDBIdx& dbi,    const string& key, ArrayReply& array){
    return command_array(dbi, array, "HGETALL %s", key.c_str());
}

bool xRedisClient::hincrby(const RedisDBIdx& dbi,  const string& key, const string& filed, const int64_t increment, int64_t& num ) {
    return command_integer(dbi, num, "HINCRBY %s %s %lld", key.c_str(),filed.c_str(), increment);
}

bool xRedisClient::hincrbyfloat(const RedisDBIdx& dbi,  const string& key, const string& filed, const float increment, float& value ) {
    bool bRet = false;
    RedisConn *pRedisConn = mRedisPool->GetConnection(dbi.mType, dbi.mIndex);
    if (NULL==pRedisConn) {
        return false;
    }

    redisReply *reply = static_cast<redisReply *>(redisCommand(pRedisConn->getCtx(), "HINCRBYFLOAT %s %s %f", key.c_str(), filed.c_str(), increment));
    if (RedisPool::CheckReply(reply)) {
        value = atof(reply->str);
        bRet = true;
    }

    RedisPool::FreeReply(reply);
    mRedisPool->FreeConnection(pRedisConn);
    return bRet;
}

bool xRedisClient::hkeys(const RedisDBIdx& dbi,  const string& key, KEYS& keys){
    return command_list(dbi, keys, "HKEYS %s", key.c_str());
}

bool xRedisClient::hlen(const RedisDBIdx& dbi,  const string& key, int64_t& count){
    return command_integer(dbi, count, "HLEN %s", key.c_str());
}

bool xRedisClient::hmget(const RedisDBIdx& dbi,    const string& key, const KEYS& filed, ArrayReply& array){
    VDATA vCmdData;
    vCmdData.push_back("HMGET");
    vCmdData.push_back(key);
    addparam(vCmdData, filed);
    return commandargv_array(dbi, vCmdData, array);
}

bool xRedisClient::hmset(const RedisDBIdx& dbi,    const string& key, const VDATA& vData){
    VDATA vCmdData;
    vCmdData.push_back("HMSET");
    vCmdData.push_back(key);
    addparam(vCmdData, vData);
    return commandargv_status(dbi, vCmdData);
}

bool xRedisClient::hset(const RedisDBIdx& dbi,    const string& key, const string& filed, const string& value, int64_t& retval){
    return command_integer(dbi, retval, "HSET %s %s %s", key.c_str(), filed.c_str(), value.c_str());
}

bool xRedisClient::hsetnx(const RedisDBIdx& dbi,    const string& key, const string& filed, const string& value){
    return command_bool(dbi, "HSETNX %s %s %s", key.c_str(), filed.c_str(), value.c_str());
}

bool xRedisClient::hvals(const RedisDBIdx& dbi,  const string& key, VALUES& values) {
    return command_list(dbi, values, "HVALS %s", key.c_str());
}


