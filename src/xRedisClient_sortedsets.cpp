/*
 * ----------------------------------------------------------------------------
 * Copyright (c) 2013-2014, xSky <guozhw at gmail dot com>
 * All rights reserved.
 * Distributed under GPL license.
 * ----------------------------------------------------------------------------
 */
#include <sstream>
#include <fmt/printf.h>
#include <fmt/format.h>

#include "xredis.h"

using namespace xrc;

bool xRedisClient::zadd(const RedisDBIdx &dbi, const KEY &key, const VALUES &vValues, int64_t &count)
{
    VDATA vCmdData;
    vCmdData.push_back("ZADD");
    vCmdData.push_back(key);
    addparam(vCmdData, vValues);
    SETDEFAULTIOTYPE(MASTER);
    return commandargv_integer(dbi, vCmdData, count);
}

bool xRedisClient::zscrad(const RedisDBIdx &dbi, const std::string &key, int64_t &count)
{
    if (0 == key.length())
    {
        return false;
    }
    SETDEFAULTIOTYPE(SLAVE);
    return command_integer(dbi, count, fmt::format("ZSCRAD {}", key).c_str());
}

bool xRedisClient::zincrby(const RedisDBIdx &dbi, const std::string &key, const double &increment, const std::string &member, std::string &value)
{
    if (0 == key.length())
    {
        return false;
    }
    SETDEFAULTIOTYPE(MASTER);
    return command_string(dbi, value, fmt::format("ZINCRBY {} {} {}", key, increment, member).c_str());
}

bool xRedisClient::zrange(const RedisDBIdx &dbi, const std::string &key, int32_t start, int32_t end, VALUES &vValues, bool withscore)
{
    if (0 == key.length())
    {
        return false;
    }
    SETDEFAULTIOTYPE(SLAVE);
    if (withscore)
    {
        return command_list(dbi, vValues, fmt::format("ZRANGE {} {} {} WITHSCORES", key, start, end).c_str());
    }
    return command_list(dbi, vValues, fmt::format("ZRANGE {} {} {}", key, start, end).c_str());
}

bool xRedisClient::zrangebyscore(const RedisDBIdx &dbi, const std::string &key, const std::string &min,
                                 const std::string &max, VALUES &vValues, bool withscore, LIMIT *limit /*= NULL*/)
{
    if (0 == key.length())
    {
        return false;
    }

    VDATA vCmdData;
    vCmdData.push_back("ZRANGEBYSCORE");
    vCmdData.push_back(key);
    vCmdData.push_back(min);
    vCmdData.push_back(max);

    if (withscore)
    {
        vCmdData.push_back("WITHSCORES");
    }

    if (NULL != limit)
    {
        vCmdData.push_back("LIMIT");
        vCmdData.push_back(toString(limit->offset));
        vCmdData.push_back(toString(limit->count));
    }

    SETDEFAULTIOTYPE(SLAVE);
    return commandargv_array(dbi, vCmdData, vValues);
}

bool xRedisClient::zrank(const RedisDBIdx &dbi, const std::string &key, const std::string &member, int64_t &rank)
{
    if (0 == key.length())
    {
        return false;
    }
    SETDEFAULTIOTYPE(MASTER);
    return command_integer(dbi, rank, fmt::format("ZRANK {} {}", key, member).c_str());
}

bool xRedisClient::zrem(const RedisDBIdx &dbi, const KEY &key, const VALUES &vmembers, int64_t &count)
{
    VDATA vCmdData;
    vCmdData.push_back("ZREM");
    vCmdData.push_back(key);
    addparam(vCmdData, vmembers);
    SETDEFAULTIOTYPE(MASTER);
    return commandargv_integer(dbi, vCmdData, count);
}

bool xRedisClient::zremrangebyrank(const RedisDBIdx &dbi, const std::string &key, int32_t start, int32_t stop, int64_t &count)
{
    if (0 == key.length())
    {
        return false;
    }
    SETDEFAULTIOTYPE(MASTER);
    return command_integer(dbi, count, fmt::format("ZREMRANGEBYRANK {} {} {}", key, start, stop).c_str());
}

bool xRedisClient::zremrangebyscore(const RedisDBIdx &dbi, const KEY &key, double min, double max, int64_t &count)
{
    if (0 == key.length())
    {
        return false;
    }
    SETDEFAULTIOTYPE(SLAVE);
    return command_integer(dbi, count, fmt::format("ZREMRANGEBYSCORE {} {} {}", key, min, max).c_str());
}

bool xRedisClient::zrevrange(const RedisDBIdx &dbi, const std::string &key, int32_t start, int32_t end, VALUES &vValues, bool withscore)
{
    if (0 == key.length())
    {
        return false;
    }
    if (withscore)
    {
        return command_list(dbi, vValues, fmt::format("ZREVRANGE {} {} {} {}", key, start, end, "WITHSCORES").c_str());
    }
    return command_list(dbi, vValues, fmt::format("ZREVRANGE {} {} {}", key, start, end).c_str());
}

bool xRedisClient::zrevrank(const RedisDBIdx &dbi, const std::string &key, const std::string &member, int64_t &rank)
{
    if (0 == key.length())
    {
        return false;
    }
    SETDEFAULTIOTYPE(SLAVE);
    return command_integer(dbi, rank, fmt::format("ZREVRANK {} {}", key, member).c_str());
}

bool xRedisClient::zscan(const RedisDBIdx &dbi, const std::string &key, int64_t &cursor, const char *pattern,
                         uint32_t count, ArrayReply &array, xRedisContext &ctx)
{
    return ScanFun("ZSCAN", dbi, &key, cursor, pattern, count, array, ctx);
}

bool xRedisClient::zscore(const RedisDBIdx &dbi, const std::string &key, const std::string &member, std::string &score)
{
    if (0 == key.length())
    {
        return false;
    }
    SETDEFAULTIOTYPE(SLAVE);
    return command_string(dbi, score, fmt::format("ZSCORE {} {}", key, member).c_str());
}
