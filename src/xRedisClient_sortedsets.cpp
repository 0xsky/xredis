/*
 * ----------------------------------------------------------------------------
 * Copyright (c) 2013-2014, xSky <guozhw at gmail dot com>
 * All rights reserved.
 * Distributed under GPL license.
 * ----------------------------------------------------------------------------
 */

#include "xRedisClient.h"
#include <sstream>


bool xRedisClient::zadd(const RedisDBIdx& dbi, const KEY& key,   const VALUES& vValues, int64_t& count){
    VDATA vCmdData;
    vCmdData.push_back("ZADD");
    vCmdData.push_back(key);
    addparam(vCmdData, vValues);
    SETDEFAULTIOTYPE(MASTER);
    return commandargv_integer(dbi, vCmdData, count);
}

bool xRedisClient::zscrad(const RedisDBIdx& dbi,     const string& key, int64_t& count){
    if (0==key.length()) {
        return false;
    }
    SETDEFAULTIOTYPE(SLAVE);
    return command_integer(dbi, count, "ZSCRAD %s", key.c_str());
}

bool xRedisClient::zincrby(const RedisDBIdx& dbi,  const string& key, const double &increment, const string& member, string& value ) {
    if (0==key.length()) {
        return false;
    }
    SETDEFAULTIOTYPE(MASTER);
    return command_string(dbi, value, "ZINCRBY %s %f %s", key.c_str(), increment, member.c_str());
}

bool xRedisClient::zrange(const RedisDBIdx& dbi,  const string& key, int start, int end, VALUES& vValues, bool withscore) {
    if (0==key.length()) {
        return false;
    }
    SETDEFAULTIOTYPE(SLAVE);
    if (withscore) {
        return command_list(dbi, vValues, "ZRANGE %s %d %d %s", key.c_str(), start, end, "WITHSCORES");
    }
    return command_list(dbi, vValues, "ZRANGE %s %d %d", key.c_str(), start, end);
}

bool xRedisClient::zrank(const RedisDBIdx& dbi,  const string& key, const string& member, int64_t &rank) {
    if (0==key.length()) {
        return false;
    }
    SETDEFAULTIOTYPE(MASTER);
    return command_integer(dbi, rank, "ZRANK %s %s", key.c_str(), member.c_str());
}

bool xRedisClient::zrem(const RedisDBIdx& dbi,        const KEY& key, const VALUES& vmembers, int64_t &count) {
    VDATA vCmdData;
    vCmdData.push_back("ZREM");
    vCmdData.push_back(key);
    addparam(vCmdData, vmembers);
    SETDEFAULTIOTYPE(MASTER);
    return commandargv_integer(dbi, vCmdData, count);
}

bool xRedisClient::zremrangebyrank(const RedisDBIdx& dbi,  const string& key, int start, int stop, int64_t& count) {
    if (0==key.length()) {
        return false;
    }
    SETDEFAULTIOTYPE(MASTER);
    return command_integer(dbi, count, "ZREMRANGEBYRANK %s %d %d", key.c_str(), start, stop);
}

bool xRedisClient::zrevrange(const RedisDBIdx& dbi,  const string& key, int start, int end, VALUES& vValues, bool withscore) {
    if (0==key.length()) {
        return false;
    }
    if (withscore) {
        return command_list(dbi, vValues, "ZREVRANGE %s %d %d %s", key.c_str(), start, end, "WITHSCORES");
    }
    return command_list(dbi, vValues, "ZREVRANGE %s %d %d", key.c_str(), start, end);
}

 bool xRedisClient::zrevrank(const RedisDBIdx& dbi,  const string& key, const string &member, int64_t& rank){
     if (0==key.length()) {
         return false;
     }
     SETDEFAULTIOTYPE(SLAVE);
     return command_integer(dbi, rank, "ZREVRANK %s %s", key.c_str(), member.c_str());
 }

 bool xRedisClient::zscan(const RedisDBIdx& dbi, const std::string& key, int64_t &cursor, const char *pattern,
     uint32_t count, ArrayReply& array, xRedisContext& ctx)
 {
     return ScanFun("ZSCAN", dbi, &key, cursor, pattern, count, array, ctx);
 }

 bool xRedisClient::zscore(const RedisDBIdx& dbi,  const string& key, const string &member, string& score ){
     if (0==key.length()) {
         return false;
     }
     SETDEFAULTIOTYPE(SLAVE);
     return command_string(dbi, score, "ZSCORE %s %s", key.c_str(), member.c_str());
 }


