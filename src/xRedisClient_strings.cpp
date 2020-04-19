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

bool xRedisClient::psetex(const RedisDBIdx &dbi, const std::string &key, int32_t milliseconds, const std::string &value)
{
    SETDEFAULTIOTYPE(MASTER);
    return command_bool(dbi, fmt::format("PSETEX {} {} {}", key, milliseconds, value).c_str());
}

bool xRedisClient::append(const RedisDBIdx &dbi, const std::string &key, const std::string &value)
{
    VDATA vCmdData;
    vCmdData.push_back("APPEND");
    vCmdData.push_back(key);
    vCmdData.push_back(value);
    SETDEFAULTIOTYPE(MASTER);
    return commandargv_status(dbi, vCmdData);
}

bool xRedisClient::set(const RedisDBIdx &dbi, const std::string &key, const std::string &value)
{
    VDATA vCmdData;
    vCmdData.push_back("SET");
    vCmdData.push_back(key);
    vCmdData.push_back(value);
    SETDEFAULTIOTYPE(MASTER);
    return commandargv_status(dbi, vCmdData);
}

bool xRedisClient::set(const RedisDBIdx &dbi, const std::string &key, const std::string &value, SETPXEX pxex, int32_t expiretime, SETNXXX nxxx)
{
    static const char *pXflag[] = {"px", "ex", "nx", "xx"};
    SETDEFAULTIOTYPE(MASTER);

    VDATA vCmdData;
    vCmdData.push_back("SET");
    vCmdData.push_back(key);
    vCmdData.push_back(value);

    if (pxex > 0)
    {
        vCmdData.push_back((pxex == PX) ? pXflag[0] : pXflag[1]);
        vCmdData.push_back(toString(expiretime));
    }

    if (nxxx > 0)
    {
        vCmdData.push_back((nxxx == NX) ? pXflag[2] : pXflag[3]);
    }

    return commandargv_status(dbi, vCmdData);
}

bool xRedisClient::set(const RedisDBIdx &dbi, const std::string &key, const char *value, int32_t len, int32_t second)
{
    SETDEFAULTIOTYPE(MASTER);

    // use std::string_view here?
    if (0 == second)
    {
        return command_bool(dbi, "set %s %b", key, value, len);
    }
    else
    {
        return command_bool(dbi, "set %s %b EX %s", key, value, len, second);
    }
}

bool xRedisClient::setbit(const RedisDBIdx &dbi, const std::string &key, int32_t offset, int64_t newbitValue, int64_t oldbitValue)
{
    SETDEFAULTIOTYPE(MASTER);
    return command_integer(dbi, oldbitValue, fmt::format("SETBIT {} {} {}", key, offset, newbitValue).c_str());
}

bool xRedisClient::get(const RedisDBIdx &dbi, const std::string &key, std::string &value)
{
    SETDEFAULTIOTYPE(SLAVE);
    return command_string(dbi, value, fmt::format("GET {}", key).c_str());
}

bool xRedisClient::getbit(const RedisDBIdx &dbi, const std::string &key, int32_t &offset, int32_t &bit)
{
    SETDEFAULTIOTYPE(SLAVE);
    int64_t intval = 0;
    bool bRet = command_integer(dbi, intval, fmt::format("GETBIT {} {}", key, offset).c_str());
    bit = (int32_t)intval;
    return bRet;
}

bool xRedisClient::getrange(const RedisDBIdx &dbi, const std::string &key, int32_t start, int32_t end, std::string &out)
{
    SETDEFAULTIOTYPE(SLAVE);
    return command_string(dbi, out, fmt::format("GETRANGE {} {} {}", key, start, end).c_str());
}

bool xRedisClient::getset(const RedisDBIdx &dbi, const std::string &key, const std::string &newValue, std::string &oldValue)
{
    SETDEFAULTIOTYPE(MASTER);
    return command_string(dbi, oldValue, fmt::format("GETSET {} {}", key, newValue).c_str());
}

bool xRedisClient::mget(const DBIArray &vdbi, const KEYS &keys, ReplyData &vDdata)
{
    bool bRet = false;
    size_t n = vdbi.size();
    if (n != keys.size())
    {
        return bRet;
    }

    DataItem item;
    DBIArray::const_iterator iter_dbi = vdbi.begin();
    KEYS::const_iterator iter_key = keys.begin();
    for (; iter_key != keys.end(); ++iter_key, ++iter_dbi)
    {
        const RedisDBIdx &dbi = *iter_dbi;
        SETDEFAULTIOTYPE(SLAVE);
        const std::string &key = *iter_key;
        if (key.length() > 0)
        {
            bool ret = command_string(*iter_dbi, item.str, fmt::format("GET {}", key).c_str());
            if (!ret)
            {
                item.type = REDIS_REPLY_NIL;
                item.str = "";
            }
            else
            {
                item.type = REDIS_REPLY_STRING;
                bRet = true;
            }
            vDdata.push_back(item);
        }
    }
    return bRet;
}

bool xRedisClient::mset(const DBIArray &vdbi, const VDATA &vData)
{
    DBIArray::const_iterator iter_dbi = vdbi.begin();
    VDATA::const_iterator iter_data = vData.begin();
    for (; iter_data != vData.end(); iter_dbi++)
    {
        const std::string &key = (*iter_data++);
        const std::string &value = (*iter_data++);
        const RedisDBIdx &dbi = *iter_dbi;
        SETDEFAULTIOTYPE(SLAVE);
        command_status(dbi, fmt::format("SET {} {}", key, value).c_str());
    }
    return true;
}

bool xRedisClient::setex(const RedisDBIdx &dbi, const std::string &key, int32_t seconds, const std::string &value)
{
    VDATA vCmdData;

    vCmdData.push_back("SETEX");
    vCmdData.push_back(key);
    vCmdData.push_back(toString(seconds));
    vCmdData.push_back(value);
    SETDEFAULTIOTYPE(MASTER);
    return commandargv_status(dbi, vCmdData);
}

bool xRedisClient::setnx(const RedisDBIdx &dbi, const std::string &key, const std::string &value)
{
    VDATA vCmdData;
    vCmdData.push_back("SETNX");
    vCmdData.push_back(key);
    vCmdData.push_back(value);
    SETDEFAULTIOTYPE(MASTER);
    return commandargv_bool(dbi, vCmdData);
}

bool xRedisClient::setrange(const RedisDBIdx &dbi, const std::string &key, int32_t offset, const std::string &value, int32_t &length)
{
    int64_t intval = 0;
    SETDEFAULTIOTYPE(MASTER);
    bool bRet = command_integer(dbi, intval, fmt::format("setrange {} {} {}", key, offset, value).c_str());
    length = (int32_t)intval;
    return bRet;
}

bool xRedisClient::strlen(const RedisDBIdx &dbi, const std::string &key, int32_t &length)
{
    int64_t intval = 0;
    SETDEFAULTIOTYPE(SLAVE);
    bool bRet = command_integer(dbi, intval, fmt::format("STRLEN {}", key).c_str());
    length = (int32_t)intval;
    return bRet;
}

bool xRedisClient::incr(const RedisDBIdx &dbi, const std::string &key, int64_t &result)
{
    SETDEFAULTIOTYPE(MASTER);
    return command_integer(dbi, result, fmt::format("INCR {}", key).c_str());
}

bool xRedisClient::incrby(const RedisDBIdx &dbi, const std::string &key, int32_t by, int64_t &result)
{
    SETDEFAULTIOTYPE(MASTER);
    return command_integer(dbi, result, fmt::format("INCRBY {} {}", key, by).c_str());
}

bool xRedisClient::bitcount(const RedisDBIdx &dbi, const std::string &key, int32_t &count, int32_t start, int32_t end)
{
    int64_t intval = 0;
    bool bRet = false;
    SETDEFAULTIOTYPE(SLAVE);
    if ((start != 0) || (end != 0))
    {
        bRet = command_integer(dbi, intval, fmt::format("bitcount {} {} {}", key, start, end).c_str());
    }
    else
    {
        bRet = command_integer(dbi, intval, fmt::format("bitcount {}", key).c_str());
    }
    count = (int32_t)intval;
    return bRet;
}

//// 这个实现有问题
//bool xRedisClient::bitop(const RedisDBIdx& dbi, const BITOP operation, const string& destkey, const KEYS& keys, int32_t& lenght) {
//    static const char *op_cmd[4]= {"AND","OR","XOR","NOT"};
//    VDATA vCmdData;
//    int64_t intval = 0;
//    vCmdData.push_back("bitop");
//    vCmdData.push_back(op_cmd[operation]);
//    vCmdData.push_back(destkey);
//    addparam(vCmdData, keys);
//    SETDEFAULTIOTYPE(MASTER);
//    bool bRet = commandargv_integer(dbi, vCmdData, intval);
//    lenght = (int32_t)intval;
//    return bRet;
//}

bool xRedisClient::bitpos(const RedisDBIdx &dbi, const std::string &key, int32_t bit, int64_t &pos, int32_t start, int32_t end)
{
    SETDEFAULTIOTYPE(SLAVE);
    if ((start != 0) || (end != 0))
    {
        return command_integer(dbi, pos, fmt::format("BITPOS {} {} {} {}", key, bit, start, end).c_str());
    }
    return command_integer(dbi, pos, fmt::format("BITPOS {} {}", key.c_str(), bit).c_str());
}

bool xRedisClient::decr(const RedisDBIdx &dbi, const std::string &key, int64_t &result)
{
    SETDEFAULTIOTYPE(MASTER);
    return command_integer(dbi, result, fmt::format("decr {}", key).c_str());
}

bool xRedisClient::decrby(const RedisDBIdx &dbi, const std::string &key, int32_t by, int64_t &result)
{
    SETDEFAULTIOTYPE(MASTER);
    return command_integer(dbi, result, fmt::format("decrby {} {}", key.c_str(), by).c_str());
}
