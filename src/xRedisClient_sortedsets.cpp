/*
 * ----------------------------------------------------------------------------
 * Copyright (c) 2013-2014, xSky <guozhw at gmail dot com>
 * All rights reserved.
 * Distributed under GPL license.
 * ----------------------------------------------------------------------------
 */

#include "xRedisClient.h"
using namespace xrc;

bool xRedisClient::zadd(const SliceIndex& index, const KEY& key,   const VALUES& vValues, int64_t& count){
    VDATA vCmdData;
    vCmdData.push_back("ZADD");
    vCmdData.push_back(key);
    addparam(vCmdData, vValues);
    SETDEFAULTIOTYPE(MASTER);
    return commandargv_integer(index, vCmdData, count);
}

bool xRedisClient::zscrad(const SliceIndex& index, const std::string& key, int64_t& count){
    if (0==key.length()) {
        return false;
    }
    SETDEFAULTIOTYPE(SLAVE);
    return command_integer(index, count, "ZSCRAD %s", key.c_str());
}

bool xRedisClient::zincrby(const SliceIndex& index, const std::string& key, const double &increment, const std::string& member, std::string& value) {
    if (0==key.length()) {
        return false;
    }
    SETDEFAULTIOTYPE(MASTER);
    return command_string(index, value, "ZINCRBY %s %f %s", key.c_str(), increment, member.c_str());
}

bool xRedisClient::zpopmax(const SliceIndex& index, const std::string& key, VALUES& vValues) {
    if (0==key.length()) {
        return false;
    }
    SETDEFAULTIOTYPE(MASTER);
    return command_list(index, vValues, "ZPOPMAX %s", key.c_str());
}

bool xRedisClient::zpopmin(const SliceIndex& index, const std::string& key,  VALUES& vValues) {
    if (0==key.length()) {
        return false;
    }
    SETDEFAULTIOTYPE(MASTER);
    return command_list(index, vValues, "ZPOPMIN %s", key.c_str());
}

bool xRedisClient::zrange(const SliceIndex& index, const std::string& key, int32_t start, int32_t end, VALUES& vValues, bool withscore) {
    if (0==key.length()) {
        return false;
    }
    SETDEFAULTIOTYPE(SLAVE);
    if (withscore) {
        return command_list(index, vValues, "ZRANGE %s %d %d %s", key.c_str(), start, end, "WITHSCORES");
    }
    return command_list(index, vValues, "ZRANGE %s %d %d", key.c_str(), start, end);
}

bool xRedisClient::zrangebyscore(const SliceIndex& index, const std::string& key, const std::string& min, 
            const std::string& max, VALUES& vValues, bool withscore, LIMIT *limit /*= NULL*/) {
    if (0==key.length()) {
        return false;
    }

    VDATA vCmdData;
    vCmdData.push_back("ZRANGEBYSCORE");
    vCmdData.push_back(key);
    vCmdData.push_back(min);
    vCmdData.push_back(max);

    if (withscore) {
        vCmdData.push_back("WITHSCORES");        
    }

    if (NULL != limit) {
        vCmdData.push_back("LIMIT");
        vCmdData.push_back(toString(limit->offset));
        vCmdData.push_back(toString(limit->count));
    }

    SETDEFAULTIOTYPE(SLAVE);
    return commandargv_array(index, vCmdData, vValues);
}

bool xRedisClient::zrank(const SliceIndex& index, const std::string& key, const std::string& member, int64_t &rank) {
    if (0==key.length()) {
        return false;
    }
    SETDEFAULTIOTYPE(MASTER);
    return command_integer(index, rank, "ZRANK %s %s", key.c_str(), member.c_str());
}

bool xRedisClient::zrem(const SliceIndex& index,        const KEY& key, const VALUES& vmembers, int64_t &count) {
    VDATA vCmdData;
    vCmdData.push_back("ZREM");
    vCmdData.push_back(key);
    addparam(vCmdData, vmembers);
    SETDEFAULTIOTYPE(MASTER);
    return commandargv_integer(index, vCmdData, count);
}

bool xRedisClient::zremrangebyrank(const SliceIndex& index, const std::string& key, int32_t start, int32_t stop, int64_t& count) {
    if (0==key.length()) {
        return false;
    }
    SETDEFAULTIOTYPE(MASTER);
    return command_integer(index, count, "ZREMRANGEBYRANK %s %d %d", key.c_str(), start, stop);
}

bool xRedisClient::zremrangebyscore(const SliceIndex& index, const KEY& key, double min, double  max, int64_t& count){
    if (0==key.length()) {
        return false;
    }
    SETDEFAULTIOTYPE(SLAVE);
    return command_integer(index, count, "ZREMRANGEBYSCORE %s %f %f", key.c_str(), min, max);
}

bool xRedisClient::zrevrange(const SliceIndex& index, const std::string& key, int32_t start, int32_t end, VALUES& vValues, bool withscore) {
    if (0==key.length()) {
        return false;
    }
    if (withscore) {
        return command_list(index, vValues, "ZREVRANGE %s %d %d %s", key.c_str(), start, end, "WITHSCORES");
    }
    return command_list(index, vValues, "ZREVRANGE %s %d %d", key.c_str(), start, end);
}

bool xRedisClient::zrevrank(const SliceIndex& index, const std::string& key, const std::string &member, int64_t& rank){
     if (0==key.length()) {
         return false;
     }
     SETDEFAULTIOTYPE(SLAVE);
     return command_integer(index, rank, "ZREVRANK %s %s", key.c_str(), member.c_str());
 }

 bool xRedisClient::zscan(const SliceIndex& index, const std::string& key, int64_t &cursor, const char *pattern,
     uint32_t count, ArrayReply& array, xRedisContext& ctx)
 {
     return ScanFun("ZSCAN", index, &key, cursor, pattern, count, array, ctx);
 }

 bool xRedisClient::zscore(const SliceIndex& index, const std::string& key, const std::string &member, std::string& score){
     if (0==key.length()) {
         return false;
     }
     SETDEFAULTIOTYPE(SLAVE);
     return command_string(index, score, "ZSCORE %s %s", key.c_str(), member.c_str());
 }


