/*
 * ----------------------------------------------------------------------------
 * Copyright (c) 2013-2014, xSky <guozhw at gmail dot com>
 * All rights reserved.
 * Distributed under GPL license.
 * ----------------------------------------------------------------------------
 */

#include "xRedisClient.h"

using namespace xrc;

bool  xRedisClient::del(const SliceIndex& index, const std::string& key) {
    if (0==key.length()) {
        return false;
    }

    SETDEFAULTIOTYPE(MASTER);
    return command_bool(index, "DEL %s", key.c_str());
}

bool  xRedisClient::del(const DBIArray& vdbi,    const KEYS &  vkey, int64_t& count) {
    count = 0;
    if (vdbi.size()!=vkey.size()) {
        return false;
    }
    DBIArray::const_iterator iter_dbi = vdbi.begin();
    KEYS::const_iterator iter_key     = vkey.begin();
    for(;iter_key!=vkey.end();++iter_key, ++iter_dbi) {
        const SliceIndex &index = (*iter_dbi);
        const std::string &key = (*iter_key);
        if (del(index, key)) {
            count++;
        }
    }
    return true;
}

bool xRedisClient::exists(const SliceIndex& index, const std::string& key) {
    if (0==key.length()) {
        return false;
    }
    SETDEFAULTIOTYPE(MASTER);
    return command_bool(index, "EXISTS %s", key.c_str());
}

bool xRedisClient::expire(const SliceIndex& index, const std::string& key, uint32_t second) {
    if (0==key.length()) {
        return false;
    }
    SETDEFAULTIOTYPE(MASTER);
    int64_t ret = -1;
    if (!command_integer(index, ret, "EXPIRE %s %u", key.c_str(), second)) {
        return false;
    }

    if (1==ret) {
        return true;
    } else {
        SetErrMessage(index, "expire return %ld ", ret);
        return false;
    }
}

bool xRedisClient::expireat(const SliceIndex& index, const std::string& key, uint32_t timestamp) {
    if (0==key.length()) {
        return false;
    }
    SETDEFAULTIOTYPE(MASTER);
    return command_bool(index, "EXPIREAT %s %u", key.c_str(), timestamp);
}

bool xRedisClient::persist(const SliceIndex& index, const std::string& key) {
    if (0==key.length()) {
        return false;
    }
    SETDEFAULTIOTYPE(MASTER);
    return command_bool(index, "PERSIST %s %u", key.c_str());
}

bool xRedisClient::pexpire(const SliceIndex& index, const std::string& key, uint32_t milliseconds) {
    if (0==key.length()) {
        return false;
    }
    return command_bool(index, "PEXPIRE %s %u", key.c_str(), milliseconds);
}

bool xRedisClient::pexpireat(const SliceIndex& index, const std::string& key, uint32_t millisecondstimestamp) {
    if (0==key.length()) {
        return false;
    }
    SETDEFAULTIOTYPE(MASTER);
    return command_bool(index, "PEXPIREAT %s %u", key.c_str(), millisecondstimestamp);
}

bool xRedisClient::pttl(const SliceIndex& index, const std::string& key, int64_t &milliseconds) {
    if (0==key.length()) {
        return false;
    }
    SETDEFAULTIOTYPE(MASTER);
    return command_integer(index, milliseconds, "PTTL %s", key.c_str());
}

bool xRedisClient::ttl(const SliceIndex& index, const std::string& key, int64_t &seconds) {
    if (0==key.length()) {
        return false;
    }
    SETDEFAULTIOTYPE(SLAVE);
    return command_integer(index, seconds, "TTL %s", key.c_str());
}

bool xRedisClient::type(const SliceIndex& index, const std::string& key, std::string& value){
    SETDEFAULTIOTYPE(MASTER);
    return command_string(index, value, "TYPE %s", key.c_str());
}

bool xRedisClient::randomkey(const SliceIndex& index, KEY& key){
    SETDEFAULTIOTYPE(SLAVE);
    return command_string(index, key, "RANDOMKEY");
}

bool xRedisClient::scan(const SliceIndex& index, int64_t &cursor, const char *pattern, 
    uint32_t count, ArrayReply& array, xRedisContext& ctx)
{
    return ScanFun("SCAN",index, NULL, cursor, pattern, count, array, ctx);
}

bool xRedisClient::sort(const SliceIndex& index, ArrayReply& array, const std::string& key, const char* by,
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
    return commandargv_array(index, vCmdData, array);
}






