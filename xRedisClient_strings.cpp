/*
 * ----------------------------------------------------------------------------
 * Copyright (c) 2013-2014, xSky <guozhw at gmail dot com>
 * All rights reserved.
 * Distributed under GPL license.
 * ----------------------------------------------------------------------------
 */

#include "xRedisClient.h"
#include <sstream>

bool xRedisClient::psetex(const RedisDBIdx& dbi,    const string& key,  const int milliseconds, const string& value) {
    return command_bool(dbi, "PSETEX %s %d %s", key.c_str(), milliseconds, value.c_str());
}

bool xRedisClient::set(const RedisDBIdx& dbi,    const string& key,  const string& value) {
    VDATA vCmdData;
    vCmdData.push_back("SET");
    vCmdData.push_back(key);
    vCmdData.push_back(value);
    return commandargv_status(dbi, vCmdData);
}

bool xRedisClient::setbit(const RedisDBIdx& dbi,    const string& key,  const int offset, const int64_t newbitValue, int64_t oldbitValue) {
    return command_integer(dbi, oldbitValue, "SETBIT %s %d %lld", key.c_str(), offset, newbitValue);
}

bool xRedisClient::get(const RedisDBIdx& dbi,    const string& key,  string& value) {
    return command_string(dbi, value, "GET %s", key.c_str());
}

bool xRedisClient::getbit( const RedisDBIdx& dbi, const string& key, const int& offset, int& bit ) {
    return command_integer(dbi, (int64_t&)bit, "GETBIT %s %d", key.c_str(), offset);
}

bool xRedisClient::getrange(const RedisDBIdx& dbi,const string& key,  const int start, const int end, string& out) {
    return command_string(dbi, out, "GETRANGE %s %d %d", key.c_str(), start, end);
}

bool xRedisClient::getset(const RedisDBIdx& dbi, const string& key,  const string& newValue, string& oldValue) {
    return command_string(dbi, oldValue, "GETSET %s %s", key.c_str(), newValue.c_str());
}

bool xRedisClient::mget(const DBIArray &vdbi,   const KEYS &  keys, ReplyData& vDdata) {
    bool bRet = false;
    size_t n = vdbi.size();
    if (n!=keys.size()) {
        return bRet;
    }
    
    DataItem item;
    DBIArray::const_iterator iter_dbi = vdbi.begin();
    KEYS::const_iterator iter_key = keys.begin();
    for (;iter_key!=keys.end();++iter_key, ++iter_dbi) {
        const string &key = *iter_key;
        if (key.length()>0) {
            bool ret = command_string(*iter_dbi, item.str, "GET %s", key.c_str());
            if (ret) {
                item.type = REDIS_REPLY_NIL;
                item.str  = "";
            } else {
                item.type = REDIS_REPLY_STRING;
                bRet = true;
            }
            vDdata.push_back(item);
        }
    }

    return bRet;
}

bool xRedisClient::mset(const DBIArray& vdbi, const VDATA& vData) {
    DBIArray::const_iterator iter_dbi = vdbi.begin();
    VDATA::const_iterator iter_data = vData.begin();
    for (;iter_data!=vData.end();) {
        const string &key = (*iter_data++);
        const string &value = (*iter_data++);
        command_status(*iter_dbi++, "SET %s", key.c_str(), value.c_str());
    }
    return true;
}

bool xRedisClient::setex(const RedisDBIdx& dbi,    const string& key,  const int seconds, const string& value) {
    VDATA vCmdData;
    char szTemp[16] = {0};
    sprintf(szTemp, "%d", seconds);

    vCmdData.push_back("SETEX");
    vCmdData.push_back(key);
    vCmdData.push_back(szTemp);
    vCmdData.push_back(value);
    return commandargv_bool(dbi, vCmdData);
}

bool xRedisClient::setnx(const RedisDBIdx& dbi,  const string& key,  const string& value) {
    VDATA vCmdData;
    vCmdData.push_back("SETNX");
    vCmdData.push_back(key);
    vCmdData.push_back(value);
    return commandargv_bool(dbi, vCmdData);
}

bool xRedisClient::setrange(const RedisDBIdx& dbi,const string& key,  const int offset, const string& value, int& length) {
    return command_integer(dbi, (int64_t&)length, "setrange %s %d %s", key.c_str(), offset, value.c_str());
}

bool xRedisClient::strlen(const RedisDBIdx& dbi,const string& key, int& length) {
    return command_integer(dbi, (int64_t&)length, "STRLEN %s", key.c_str());
}

bool xRedisClient::incr(const RedisDBIdx& dbi,   const string& key, int& result) {
    return command_integer(dbi, (int64_t&)result, "INCR %s", key.c_str());
}

bool xRedisClient::incrby(const RedisDBIdx& dbi, const string& key, const int by, int& result) {
    return command_integer(dbi, (int64_t&)result, "INCR %s %d", key.c_str(), by);
}

bool xRedisClient::bitcount(const RedisDBIdx& dbi,  const string& key, int& count, const int start, const int end) {
    if ( (start!=0)||(end!=0) ) {
        return command_integer(dbi, (int64_t&)count, "bitcount %s %d %d", key.c_str(), start, end);
    } 
    return command_integer(dbi, (int64_t&)count, "bitcount %s", key.c_str());
}

bool xRedisClient::bitop(const RedisDBIdx& dbi, const BITOP operation, const string& destkey, const KEYS& keys, int& lenght) {
    static const char *op_cmd[4]= {"AND","OR","XOR","NOT"};
    VDATA vCmdData;
    vCmdData.push_back("bitop");
    vCmdData.push_back(op_cmd[operation]);
    vCmdData.push_back(destkey);
    addparam(vCmdData, keys);
    return commandargv_integer(dbi, vCmdData, (int64_t&)lenght);
}

bool xRedisClient::bitpos(const RedisDBIdx& dbi, const string& key, const int bit, int& pos, const int start, const int end) {
    if ( (start!=0)||(end!=0) ) {
        return command_integer(dbi, (int64_t&)pos, "BITPOS %s %d %d %d", key.c_str(), bit, start, end);
    } 
    return command_integer(dbi, (int64_t&)pos, "BITPOS %s %d", key.c_str(), bit);
}

bool xRedisClient::decr(const RedisDBIdx& dbi,   const string& key, int& result) {
    return command_integer(dbi,(int64_t&)result,"decr %s", key.c_str());
}

bool xRedisClient::decrby(const RedisDBIdx& dbi, const string& key, const int by, int& result) {
    return command_integer(dbi, (int64_t&)result, "decrby %s %d", key.c_str(), by);
}


















