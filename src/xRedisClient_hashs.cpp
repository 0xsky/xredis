/*
 * ----------------------------------------------------------------------------
 * Copyright (c) 2013-2014, xSky <guozhw at gmail dot com>
 * All rights reserved.
 * Distributed under GPL license.
 * ----------------------------------------------------------------------------
 */

#include "xRedisClient.h"
#include "xRedisPool.h"
#include <stdlib.h>
using namespace xrc;

bool xRedisClient::hdel(const SliceIndex& index, const std::string& key, const std::string& field, int64_t& count) {
    SETDEFAULTIOTYPE(MASTER);
    return command_integer(index, count, "HDEL %s %s", key.c_str(), field.c_str());
}

bool xRedisClient::hdel(const SliceIndex& index, const std::string& key, const KEYS& vfiled, int64_t& count) {
    VDATA vCmdData;
    vCmdData.push_back("HDEL");
    vCmdData.push_back(key);
    addparam(vCmdData, vfiled);
    SETDEFAULTIOTYPE(MASTER);
    return commandargv_integer(index, vCmdData, count);
}

bool xRedisClient::hexist(const SliceIndex& index, const std::string& key, const std::string& field){
    SETDEFAULTIOTYPE(SLAVE);
    return command_bool(index,"HEXISTS %s %s", key.c_str(), field.c_str());
}

bool xRedisClient::hget(const SliceIndex& index, const std::string& key, const std::string& field, std::string& value) {
    SETDEFAULTIOTYPE(SLAVE);
    return command_string(index, value, "HGET %s %s", key.c_str(), field.c_str());
}

bool  xRedisClient::hgetall(const SliceIndex& index, const std::string& key, ArrayReply& array){
    SETDEFAULTIOTYPE(SLAVE);
    return command_array(index, array, "HGETALL %s", key.c_str());
}

bool xRedisClient::hincrby(const SliceIndex& index, const std::string& key, const std::string& field, int64_t increment, int64_t& num) {
    SETDEFAULTIOTYPE(MASTER);
    return command_integer(index, num, "HINCRBY %s %s %lld", key.c_str(),field.c_str(), increment);
}

bool xRedisClient::hincrbyfloat(const SliceIndex& index, const std::string& key, const std::string& field, float increment, float& value) {
    SETDEFAULTIOTYPE(MASTER);
    bool bRet = false;
    RedisConn *pRedisConn = mRedisPool->GetConnection(index.mType, index.mIndex);
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

bool xRedisClient::hkeys(const SliceIndex& index, const std::string& key, KEYS& keys){
    SETDEFAULTIOTYPE(SLAVE);
    return command_list(index, keys, "HKEYS %s", key.c_str());
}

bool xRedisClient::hlen(const SliceIndex& index, const std::string& key, int64_t& count){
    SETDEFAULTIOTYPE(SLAVE);
    return command_integer(index, count, "HLEN %s", key.c_str());
}

bool xRedisClient::hmget(const SliceIndex& index, const std::string& key, const KEYS& field, ArrayReply& array){
    VDATA vCmdData;
    vCmdData.push_back("HMGET");
    vCmdData.push_back(key);
    addparam(vCmdData, field);
    SETDEFAULTIOTYPE(SLAVE);
    return commandargv_array(index, vCmdData, array);
}

bool xRedisClient::hmset(const SliceIndex& index, const std::string& key, const VDATA& vData){
    VDATA vCmdData;
    vCmdData.push_back("HMSET");
    vCmdData.push_back(key);
    addparam(vCmdData, vData);
    SETDEFAULTIOTYPE(MASTER);
    return commandargv_status(index, vCmdData);
}

bool xRedisClient::hscan(const SliceIndex& index, const std::string& key, int64_t &cursor, const char *pattern, uint32_t count, ArrayReply& array, xRedisContext& ctx)
{
    return ScanFun("HSCAN", index, &key, cursor, pattern, count, array, ctx);
}

bool xRedisClient::hset(const SliceIndex& index, const std::string& key, const std::string& field, const std::string& value, int64_t& retval){
    SETDEFAULTIOTYPE(MASTER);
    VDATA vCmdData;
    vCmdData.push_back("HSET");
    vCmdData.push_back(key);
    vCmdData.push_back(field);
    vCmdData.push_back(value);
    return commandargv_integer(index, vCmdData, retval);
    //return command_integer(index, retval, "HSET %s %s %s", key.c_str(), field.c_str(), value.c_str());
}

bool xRedisClient::hsetnx(const SliceIndex& index, const std::string& key, const std::string& field, const std::string& value){
    SETDEFAULTIOTYPE(MASTER);
    return command_bool(index, "HSETNX %s %s %s", key.c_str(), field.c_str(), value.c_str());
}

bool xRedisClient::hvals(const SliceIndex& index, const std::string& key, VALUES& values) {
    SETDEFAULTIOTYPE(SLAVE);
    return command_list(index, values, "HVALS %s", key.c_str());
}


