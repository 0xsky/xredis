/*
 * ----------------------------------------------------------------------------
 * Copyright (c) 2013-2014, xSky <guozhw at gmail dot com>
 * All rights reserved.
 * Distributed under GPL license.
 * ----------------------------------------------------------------------------
 */

#include <stdio.h> 
#include "xRedisClient.h"
using namespace xrc;

bool xRedisClient::lindex(const SliceIndex& index,    const std::string& key, int64_t idx, VALUE& value){
    if (0==key.length()) {
        return false;
    }
    SETDEFAULTIOTYPE(SLAVE);
    return command_string(index, value, "LINDEX %s %lld", key.c_str(), idx);
}

bool xRedisClient::linsert(const SliceIndex& index, const std::string& key, const LMODEL mod, const std::string& pivot, const std::string& value, int64_t& retval){
    static const char *lmodel[2]= {"BEFORE","AFTER"};
    if (0==key.length()) {
        return false;
    }
    SETDEFAULTIOTYPE(MASTER);
    return command_integer(index, retval, "LINSERT %s %s %s %s", key.c_str(), lmodel[mod], pivot.c_str(), value.c_str());
}

bool xRedisClient::llen(const SliceIndex& index, const std::string& key, int64_t& retval){
    if (0==key.length()) {
        return false;
    }
    SETDEFAULTIOTYPE(SLAVE);
    return command_integer(index, retval, "LLEN %s", key.c_str());
}

bool xRedisClient::blPop(const SliceIndex& index,    const std::string& key, VALUES& vValues, int64_t timeout)
{
    if (0==key.length()) {
        return false;
    }
    SETDEFAULTIOTYPE(MASTER);
    return command_list(index, vValues, "BLPOP %s %d", key.c_str(), (int32_t)timeout);	
}

bool xRedisClient::brPop(const SliceIndex& index,    const std::string& key, VALUES& vValues, int64_t timeout)
{
    if (0==key.length()) {
        return false;
    }
    SETDEFAULTIOTYPE(MASTER);
    return command_list(index, vValues, "BRPOP %s %d", key.c_str(), (int32_t)timeout);	
}

bool xRedisClient::brPoplpush(const SliceIndex& index, const std::string& key, std::string& targetkey, VALUE& value, int64_t timeout)
{
    if (0==key.length()) {
        return false;
    }
    SETDEFAULTIOTYPE(MASTER);
    return command_string(index, value, "BRPOPLPUSH %s %s %d", key.c_str(), targetkey.c_str(), (int32_t)timeout);	
}

bool xRedisClient::lpop(const SliceIndex& index, const std::string& key, std::string& value){
    if (0==key.length()) {
        return false;
    }
    SETDEFAULTIOTYPE(MASTER);
    return command_string(index, value, "LPOP %s", key.c_str());
}

bool xRedisClient::lpush(const SliceIndex& index, const std::string& key, const VALUES& vValue, int64_t& length){
    if (0==key.length()) {
        return false;
    }
	VDATA vCmdData;
    vCmdData.push_back("LPUSH");
    vCmdData.push_back(key);
    addparam(vCmdData, vValue);
    SETDEFAULTIOTYPE(MASTER);
    return commandargv_integer(index, vCmdData, length);
}

bool xRedisClient::lrange(const SliceIndex& index, const std::string& key, int64_t start, int64_t end, ArrayReply& array){
    if (0==key.length()) {
        return false;
    }
    SETDEFAULTIOTYPE(SLAVE);
	return command_array(index, array, "LRANGE %s %lld %lld", key.c_str(), start, end);
}

bool xRedisClient::lrem(const SliceIndex& index, const std::string& key, int32_t count, const std::string& value, int64_t num){
    if (0==key.length()) {
        return false;
    }
    SETDEFAULTIOTYPE(MASTER);
    return command_integer(index, num, "LREM %s %d %s", key.c_str(), count, value.c_str());
}

bool xRedisClient::lset(const SliceIndex& index, const std::string& key, int32_t idx, const std::string& value){
    if (0==key.length()) {
        return false;
    }
    SETDEFAULTIOTYPE(MASTER);
    return command_status(index, "LSET %s %d %s", key.c_str(), idx, value.c_str());
}

bool xRedisClient::ltrim(const SliceIndex& index, const std::string& key, int32_t start, int32_t end){
    if (0==key.length()) {
        return false;
    }
    SETDEFAULTIOTYPE(MASTER);
    return command_status(index, "LTRIM %s %d %d", key.c_str(), start, end);
}

bool xRedisClient::rpop(const SliceIndex& index, const std::string& key, std::string& value){
    if (0==key.length()) {
        return false;
    }
    SETDEFAULTIOTYPE(MASTER);
    return command_string(index, value, "RPOP %s", key.c_str());
}

bool xRedisClient::rpoplpush(const SliceIndex& index, const std::string& key_src, const std::string& key_dest, std::string& value){
    if ((0 == key_src.length()) || (0==key_dest.length())) {
        return false;
    }
    SETDEFAULTIOTYPE(MASTER);
    return command_string(index, value, "RPOPLPUSH %s %s", key_src.c_str(), key_dest.c_str());
}

bool xRedisClient::rpush(const SliceIndex& index, const std::string& key, const VALUES& vValue, int64_t& length){
    if (0==key.length()) {
        return false;
    }
	VDATA vCmdData;
    vCmdData.push_back("RPUSH");
    vCmdData.push_back(key);
    addparam(vCmdData, vValue);
    SETDEFAULTIOTYPE(MASTER);
    return commandargv_integer(index, vCmdData, length);
}

bool xRedisClient::rpushx(const SliceIndex& index, const std::string& key, const std::string& value, int64_t& length){
    if (0==key.length()) {
        return false;
    }
    SETDEFAULTIOTYPE(MASTER);
    return command_integer(index, length, "RPUSHX %s %s", key.c_str(), value.c_str());
}

