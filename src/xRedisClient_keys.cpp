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

bool  xRedisClient::del(const RedisDBIdx& dbi,    const string& key) {
    if (0==key.length()) {
        return false;
    }

    SETDEFAULTIOTYPE(MASTER);
    return command_bool(dbi, "DEL %s", key.c_str());
}

bool  xRedisClient::del(const DBIArray& vdbi,    const KEYS &  vkey, int64_t& count) {
    count = 0;
    if (vdbi.size()!=vkey.size()) {
        return false;
    }
    DBIArray::const_iterator iter_dbi = vdbi.begin();
    KEYS::const_iterator iter_key     = vkey.begin();
    for(;iter_key!=vkey.end();++iter_key, ++iter_dbi) {
        const RedisDBIdx &dbi = (*iter_dbi);
        const string &key     = (*iter_key);
        if (del(dbi, key)) {
            count++;
        }
    }
    return true;
}

bool xRedisClient::exists(const RedisDBIdx& dbi, const string& key) {
    if (0==key.length()) {
        return false;
    }
    SETDEFAULTIOTYPE(MASTER);
    return command_bool(dbi, "EXISTS %s", key.c_str());
}

bool xRedisClient::expire(const RedisDBIdx& dbi, const string& key, unsigned int second) {
    if (0==key.length()) {
        return false;
    }
    SETDEFAULTIOTYPE(MASTER);
    int64_t ret = -1;
    if (!command_integer(dbi, ret, "EXPIRE %s %u", key.c_str(), second)) {
        return false;
    }

    if (1==ret) {
        return true;
    } else {
        SetErrMessage(dbi, "expire return %ld ", ret);
        return false;
    }
}

bool xRedisClient::expireat(const RedisDBIdx& dbi, const string& key, unsigned int timestamp) {
    if (0==key.length()) {
        return false;
    }
    SETDEFAULTIOTYPE(MASTER);
    return command_bool(dbi, "EXPIREAT %s %u", key.c_str(), timestamp);
}

bool xRedisClient::persist(const RedisDBIdx& dbi, const string& key) {
    if (0==key.length()) {
        return false;
    }
    SETDEFAULTIOTYPE(MASTER);
    return command_bool(dbi, "PERSIST %s %u", key.c_str());
}

bool xRedisClient::pexpire(const RedisDBIdx& dbi, const string& key, unsigned int milliseconds) {
    if (0==key.length()) {
        return false;
    }
    return command_bool(dbi, "PEXPIRE %s %u", key.c_str(), milliseconds);
}

bool xRedisClient::pexpireat(const RedisDBIdx& dbi, const string& key, unsigned int millisecondstimestamp) {
    if (0==key.length()) {
        return false;
    }
    SETDEFAULTIOTYPE(MASTER);
    return command_bool(dbi, "PEXPIREAT %s %u", key.c_str(), millisecondstimestamp);
}

bool xRedisClient::pttl(const RedisDBIdx& dbi, const string& key, int64_t &milliseconds) {
    if (0==key.length()) {
        return false;
    }
    SETDEFAULTIOTYPE(MASTER);
    return command_integer(dbi, milliseconds, "PTTL %s", key.c_str());
}

bool xRedisClient::ttl(const RedisDBIdx& dbi, const string& key, int64_t &seconds) {
    if (0==key.length()) {
        return false;
    }
    SETDEFAULTIOTYPE(SLAVE);
    return command_integer(dbi, seconds, "TTL %s", key.c_str());
}

bool xRedisClient::type(const RedisDBIdx& dbi, const std::string& key, std::string& value){
    SETDEFAULTIOTYPE(MASTER);
    return command_string(dbi, value, "TYPE %s", key.c_str());
}

bool xRedisClient::randomkey(const RedisDBIdx& dbi, KEY& key){
    SETDEFAULTIOTYPE(SLAVE);
    return command_string(dbi, key, "RANDOMKEY");
}

bool xRedisClient::scan(const RedisDBIdx& dbi, int64_t &cursor, const char *pattern, 
    uint32_t count, ArrayReply& array, xRedisContext& ctx)
{
    return ScanFun("SCAN",dbi, NULL, cursor, pattern, count, array, ctx);
}

bool xRedisClient::sort(const RedisDBIdx& dbi, ArrayReply& array, const string& key, const char* by,
    LIMIT *limit /*= NULL*/, bool alpha /*= false*/, const FILEDS* get /*= NULL*/,
    const SORTODER order /*= ASC*/, const char* destination )
{
    static const char *sort_order[3] = { "ASC", "DESC" };
    if (0 == key.length()) {
        return false;
    }
       

    VDATA vCmdData;
    vCmdData.push_back("sort");
    vCmdData.push_back(key);
    if (NULL != by) {
        vCmdData.push_back("by");
        vCmdData.push_back(by);
    }

    if (NULL != limit) {
        vCmdData.push_back("LIMIT");
        vCmdData.push_back(toString(limit->offset));
        vCmdData.push_back(toString(limit->count));
    }
    if (alpha) {
        vCmdData.push_back("ALPHA");
    }

    if (NULL != get) {
        for (FILEDS::const_iterator iter = get->begin(); iter != get->end(); ++iter) {
            vCmdData.push_back("get");
            vCmdData.push_back(*iter);
        }
    }

    vCmdData.push_back(sort_order[order]);
    if (destination) {
        vCmdData.push_back(destination);
    }
    SETDEFAULTIOTYPE(MASTER);
    return commandargv_array(dbi, vCmdData, array);
}






