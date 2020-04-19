/*
 * ----------------------------------------------------------------------------
 * Copyright (c) 2013-2014, xSky <guozhw at gmail dot com>
 * All rights reserved.
 * Distributed under GPL license.
 * ----------------------------------------------------------------------------
 */
#include <cstdio>
#include <sstream>

#include <fmt/printf.h>
#include <fmt/format.h>

#include "xredis.h"
using namespace xrc;

bool xRedisClient::lindex(const RedisDBIdx &dbi, const std::string &key, int64_t index, VALUE &value)
{
    if (0 == key.length())
    {
        return false;
    }
    SETDEFAULTIOTYPE(SLAVE);
    return command_string(dbi, value, fmt::format("LINDEX {} {}", key, index).c_str());
}

bool xRedisClient::linsert(const RedisDBIdx &dbi, const std::string &key, const LMODEL mod, const std::string &pivot, const std::string &value, int64_t &retval)
{
    static const char *lmodel[2] = {"BEFORE", "AFTER"};
    if (0 == key.length())
    {
        return false;
    }
    SETDEFAULTIOTYPE(MASTER);
    return command_integer(dbi, retval, fmt::format("LINSERT {} {} {} {}", key, lmodel[mod], pivot, value).c_str());
}

bool xRedisClient::llen(const RedisDBIdx &dbi, const std::string &key, int64_t &retval)
{
    if (0 == key.length())
    {
        return false;
    }
    SETDEFAULTIOTYPE(SLAVE);
    return command_integer(dbi, retval, fmt::format("LLEN {}", key).c_str());
}

bool xRedisClient::blPop(const RedisDBIdx &dbi, const std::string &key, VALUES &vValues, int64_t timeout)
{
    if (0 == key.length())
    {
        return false;
    }
    SETDEFAULTIOTYPE(MASTER);
    return command_list(dbi, vValues, fmt::format("BLPOP {} {}", key, (int32_t)timeout).c_str());
}

bool xRedisClient::brPop(const RedisDBIdx &dbi, const std::string &key, VALUES &vValues, int64_t timeout)
{
    if (0 == key.length())
    {
        return false;
    }
    SETDEFAULTIOTYPE(MASTER);
    return command_list(dbi, vValues, fmt::format("BRPOP {} {}", key, (int32_t)timeout).c_str());
}

bool xRedisClient::brPoplpush(const RedisDBIdx &dbi, const std::string &key, std::string &targetkey, VALUE &value, int64_t timeout)
{
    if (0 == key.length())
    {
        return false;
    }
    SETDEFAULTIOTYPE(MASTER);
    return command_string(dbi, value, fmt::format("BRPOPLPUSH {} {}", key, targetkey, (int32_t)timeout).c_str());
}

bool xRedisClient::lpop(const RedisDBIdx &dbi, const std::string &key, std::string &value)
{
    if (0 == key.length())
    {
        return false;
    }
    SETDEFAULTIOTYPE(MASTER);
    return command_string(dbi, value, fmt::format("LPOP {}", key).c_str());
}

bool xRedisClient::lpush(const RedisDBIdx &dbi, const std::string &key, const VALUES &vValue, int64_t &length)
{
    if (0 == key.length())
    {
        return false;
    }
    VDATA vCmdData;
    vCmdData.push_back("LPUSH");
    vCmdData.push_back(key);
    addparam(vCmdData, vValue);
    SETDEFAULTIOTYPE(MASTER);
    return commandargv_integer(dbi, vCmdData, length);
}

bool xRedisClient::lrange(const RedisDBIdx &dbi, const std::string &key, int64_t start, int64_t end, ArrayReply &array)
{
    if (0 == key.length())
    {
        return false;
    }
    SETDEFAULTIOTYPE(SLAVE);
    return command_array(dbi, array, fmt::format("LRANGE {} {} {}", key, start, end).c_str());
}

bool xRedisClient::lrem(const RedisDBIdx &dbi, const std::string &key, int32_t count, const std::string &value, int64_t num)
{
    if (0 == key.length())
    {
        return false;
    }
    SETDEFAULTIOTYPE(MASTER);
    return command_integer(dbi, num, fmt::format("LREM {} {} {}", key, count, value).c_str());
}

bool xRedisClient::lset(const RedisDBIdx &dbi, const std::string &key, int32_t index, const std::string &value)
{
    if (0 == key.length())
    {
        return false;
    }
    SETDEFAULTIOTYPE(MASTER);
    return command_status(dbi, fmt::format("LSET {} {} {}", key, index, value).c_str());
}

bool xRedisClient::ltrim(const RedisDBIdx &dbi, const std::string &key, int32_t start, int32_t end)
{
    if (0 == key.length())
    {
        return false;
    }
    SETDEFAULTIOTYPE(MASTER);
    return command_status(dbi, fmt::format("LTRIM {} {} {}", key, start, end).c_str());
}

bool xRedisClient::rpop(const RedisDBIdx &dbi, const std::string &key, std::string &value)
{
    if (0 == key.length())
    {
        return false;
    }
    SETDEFAULTIOTYPE(MASTER);
    return command_string(dbi, value, fmt::format("RPOP {}", key).c_str());
}

bool xRedisClient::rpoplpush(const RedisDBIdx &dbi, const std::string &key_src, const std::string &key_dest, std::string &value)
{
    if ((0 == key_src.length()) || (0 == key_dest.length()))
    {
        return false;
    }
    SETDEFAULTIOTYPE(MASTER);
    return command_string(dbi, value, fmt::format("RPOPLPUSH {} {}", key_src, key_dest).c_str());
}

bool xRedisClient::rpush(const RedisDBIdx &dbi, const std::string &key, const VALUES &vValue, int64_t &length)
{
    if (0 == key.length())
    {
        return false;
    }
    VDATA vCmdData;
    vCmdData.push_back("RPUSH");
    vCmdData.push_back(key);
    addparam(vCmdData, vValue);
    SETDEFAULTIOTYPE(MASTER);
    return commandargv_integer(dbi, vCmdData, length);
}

bool xRedisClient::rpushx(const RedisDBIdx &dbi, const std::string &key, const std::string &value, int64_t &length)
{
    if (0 == key.length())
    {
        return false;
    }
    SETDEFAULTIOTYPE(MASTER);
    return command_integer(dbi, length, fmt::format("RPUSHX {} {}", key, value).c_str());
}
