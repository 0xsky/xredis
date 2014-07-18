/*
 * ----------------------------------------------------------------------------
 * Copyright (c) 2013-2014, xSky <guozhw at gmail dot com>
 * All rights reserved.
 * Distributed under GPL license.
 * ----------------------------------------------------------------------------
 */

#include "xRedisClient.h"
#include <sstream>

bool  xRedisClient::del(const RedisDBIdx& dbi,    const string& key) {
    return command_bool(dbi, "DEL %s", key.c_str());
}

bool  xRedisClient::del(const DBIArray& vdbi,    const KEYS &  vkey, int64_t& count) {
    count = 0;
    if (vdbi.size()!=vkey.size()) {
        return false;
    }
    DBIArray::const_iterator iter_dbi = vdbi.begin();
    KEYS::const_iterator iter_key = vkey.begin();
    for(;iter_key!=vkey.end();++iter_key, ++iter_dbi) {
        const RedisDBIdx &dbi = (*iter_dbi);
        const string &key = (*iter_key);
        if (del(dbi, key)) {
            count++;
        }
    }
    return true;
}

bool xRedisClient::exists(const RedisDBIdx& dbi, const string& key) {
    return command_bool(dbi, "EXISTS %s", key.c_str());
}

bool xRedisClient::expire(const RedisDBIdx& dbi, const string& key, const unsigned int second) {
    return command_bool(dbi, "EXPIRE %s %u", key.c_str(), second);
}

bool xRedisClient::expireat(const RedisDBIdx& dbi, const string& key, const unsigned int timestamp) {
    return command_bool(dbi, "EXPIREAT %s %u", key.c_str(), timestamp);
}

bool xRedisClient::persist(const RedisDBIdx& dbi, const string& key) {
    return command_bool(dbi, "PERSIST %s %u", key.c_str());
}

bool xRedisClient::pexpire(const RedisDBIdx& dbi, const string& key, const unsigned int milliseconds) {
    return command_bool(dbi, "PEXPIRE %s %u", key.c_str(), milliseconds);
}

bool xRedisClient::pexpireat(const RedisDBIdx& dbi, const string& key, const unsigned int millisecondstimestamp) {
    return command_bool(dbi, "PEXPIREAT %s %u", key.c_str(), millisecondstimestamp);
}

bool xRedisClient::pttl(const RedisDBIdx& dbi, const string& key, int64_t &milliseconds) {
    return command_integer(dbi, milliseconds, "PEXPIREAT %s", key.c_str());
}

bool xRedisClient::ttl(const RedisDBIdx& dbi, const string& key, int64_t &seconds) {
    return command_integer(dbi, seconds, "TTL %s", key.c_str());
}

bool xRedisClient::randomkey(const RedisDBIdx& dbi, KEY& key){
    return command_string(dbi, key, "RANDOMKEY");
}







