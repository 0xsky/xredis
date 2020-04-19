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

bool xRedisClient::del(const RedisDBIdx &dbi, const std::string &key)
{
    if (0 == key.length())
    {
        return false;
    }

    SETDEFAULTIOTYPE(MASTER);
    return command_bool(dbi, fmt::format("DEL {}", key).c_str());
}

bool xRedisClient::del(const DBIArray &vdbi, const KEYS &vkey, int64_t &count)
{
    count = 0;
    if (vdbi.size() != vkey.size())
    {
        return false;
    }
    DBIArray::const_iterator iter_dbi = vdbi.begin();
    KEYS::const_iterator iter_key = vkey.begin();
    for (; iter_key != vkey.end(); ++iter_key, ++iter_dbi)
    {
        const RedisDBIdx &dbi = (*iter_dbi);
        const std::string &key = (*iter_key);
        if (del(dbi, key))
        {
            count++;
        }
    }
    return true;
}

bool xRedisClient::exists(const RedisDBIdx &dbi, const std::string &key)
{
    if (0 == key.length())
    {
        return false;
    }
    SETDEFAULTIOTYPE(MASTER);
    return command_bool(dbi, fmt::format("EXISTS {}", key).c_str());
}

bool xRedisClient::expire(const RedisDBIdx &dbi, const std::string &key, uint32_t second)
{
    if (0 == key.length())
    {
        return false;
    }
    SETDEFAULTIOTYPE(MASTER);
    int64_t ret = -1;
    if (!command_integer(dbi, ret, fmt::format("EXPIRE {} {}", key, second).c_str()))
    {
        return false;
    }

    if (1 == ret)
    {
        return true;
    }
    else
    {
        SetErrMessage(dbi, "expire return {} ", ret);
        return false;
    }
}

bool xRedisClient::expireat(const RedisDBIdx &dbi, const std::string &key, uint32_t timestamp)
{
    if (0 == key.length())
    {
        return false;
    }
    SETDEFAULTIOTYPE(MASTER);
    return command_bool(dbi, fmt::format("EXPIREAT {} {}", key, timestamp).c_str());
}

bool xRedisClient::persist(const RedisDBIdx &dbi, const std::string &key)
{
    if (0 == key.length())
    {
        return false;
    }
    SETDEFAULTIOTYPE(MASTER);
    return command_bool(dbi, fmt::format("PERSIST {}", key).c_str());
}

bool xRedisClient::pexpire(const RedisDBIdx &dbi, const std::string &key, uint32_t milliseconds)
{
    if (0 == key.length())
    {
        return false;
    }
    return command_bool(dbi, fmt::format("PEXPIRE {} {}", key, milliseconds).c_str());
}

bool xRedisClient::pexpireat(const RedisDBIdx &dbi, const std::string &key, uint32_t millisecondstimestamp)
{
    if (0 == key.length())
    {
        return false;
    }
    SETDEFAULTIOTYPE(MASTER);
    return command_bool(dbi, fmt::format("PEXPIREAT {} {}", key, millisecondstimestamp).c_str());
}

bool xRedisClient::pttl(const RedisDBIdx &dbi, const std::string &key, int64_t &milliseconds)
{
    if (0 == key.length())
    {
        return false;
    }
    SETDEFAULTIOTYPE(MASTER);
    return command_integer(dbi, milliseconds, fmt::format("PTTL {}", key).c_str());
}

bool xRedisClient::ttl(const RedisDBIdx &dbi, const std::string &key, int64_t &seconds)
{
    if (0 == key.length())
    {
        return false;
    }
    SETDEFAULTIOTYPE(SLAVE);
    return command_integer(dbi, seconds, fmt::format("TTL {}", key).c_str());
}

bool xRedisClient::type(const RedisDBIdx &dbi, const std::string &key, std::string &value)
{
    SETDEFAULTIOTYPE(MASTER);
    return command_string(dbi, value, fmt::format("TYPE {}", key).c_str());
}

bool xRedisClient::randomkey(const RedisDBIdx &dbi, KEY &key)
{
    SETDEFAULTIOTYPE(SLAVE);
    return command_string(dbi, key, "RANDOMKEY");
}

bool xRedisClient::scan(const RedisDBIdx &dbi, int64_t &cursor, const char *pattern,
                        uint32_t count, ArrayReply &array, xRedisContext &ctx)
{
    return ScanFun("SCAN", dbi, NULL, cursor, pattern, count, array, ctx);
}

bool xRedisClient::sort(const RedisDBIdx &dbi, ArrayReply &array, const std::string &key, const char *by,
                        LIMIT *limit /*= NULL*/, bool alpha /*= false*/, const FILEDS *get /*= NULL*/,
                        const SORTODER order /*= ASC*/, const char *destination)
{
    static const char *sort_order[3] = {"ASC", "DESC"};
    if (0 == key.length())
    {
        return false;
    }

    VDATA vCmdData;
    vCmdData.push_back("sort");
    vCmdData.push_back(key);
    if (NULL != by)
    {
        vCmdData.push_back("by");
        vCmdData.push_back(by);
    }

    if (NULL != limit)
    {
        vCmdData.push_back("LIMIT");
        vCmdData.push_back(toString(limit->offset));
        vCmdData.push_back(toString(limit->count));
    }
    if (alpha)
    {
        vCmdData.push_back("ALPHA");
    }

    if (NULL != get)
    {
        for (FILEDS::const_iterator iter = get->begin(); iter != get->end(); ++iter)
        {
            vCmdData.push_back("get");
            vCmdData.push_back(*iter);
        }
    }

    vCmdData.push_back(sort_order[order]);
    if (destination)
    {
        vCmdData.push_back(destination);
    }
    SETDEFAULTIOTYPE(MASTER);
    return commandargv_array(dbi, vCmdData, array);
}
