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

bool xRedisClient::psubscribe(const RedisDBIdx& dbi, const KEYS& patterns, xRedisContext& ctx) {
    SETDEFAULTIOTYPE(MASTER);
    VDATA vCmdData;
    vCmdData.push_back("PSUBSCRIBE");
    addparam(vCmdData, patterns);
    return commandargv_array_ex(dbi, vCmdData, ctx);
}

bool xRedisClient::publish(const RedisDBIdx& dbi,  const KEY& channel, const std::string& message, int64_t& count) {
    //SETDEFAULTIOTYPE(MASTER);
    //return command_integer(dbi, count, "PUBLISH %s %s", channel.c_str(), message.c_str(), count);

    SETDEFAULTIOTYPE(MASTER);
    VDATA vCmdData;
    vCmdData.push_back("PUBLISH");
    vCmdData.push_back(channel);
    vCmdData.push_back(message);
    return commandargv_integer(dbi, vCmdData, count);
}

bool xRedisClient::pubsub_channels(const RedisDBIdx& dbi, const std::string &pattern, ArrayReply &reply) {
    SETDEFAULTIOTYPE(MASTER);
    return command_array(dbi, reply, "pubsub channels %s", pattern.c_str());
}

bool xRedisClient::pubsub_numsub(const RedisDBIdx& dbi, const KEYS &keys, ArrayReply &reply) {
    SETDEFAULTIOTYPE(MASTER);
    VDATA vCmdData;
    vCmdData.push_back("pubsub");
    vCmdData.push_back("numsub");
    addparam(vCmdData, keys);
    return commandargv_array(dbi, vCmdData, reply);
}

bool xRedisClient::pubsub_numpat(const RedisDBIdx& dbi, int64_t& count) {
    SETDEFAULTIOTYPE(MASTER);
    return command_integer(dbi, count, "pubsub numpat");
}

bool xRedisClient::punsubscribe(const RedisDBIdx& dbi, const KEYS& patterns, xRedisContext& ctx) {
    SETDEFAULTIOTYPE(MASTER);
    VDATA vCmdData;
    vCmdData.push_back("PUNSUBSCRIBE");
    addparam(vCmdData, patterns);

    bool bRet = false;
    RedisConn *pRedisConn = static_cast<RedisConn *>(ctx.conn);
    if (NULL == pRedisConn) {
        SetErrString(dbi, GET_CONNECT_ERROR, ::strlen(GET_CONNECT_ERROR));
        return false;
    }

    vector<const char*> argv(vCmdData.size());
    vector<size_t> argvlen(vCmdData.size());
    unsigned int j = 0;
    for (VDATA::const_iterator i = vCmdData.begin(); i != vCmdData.end(); ++i, ++j) {
        argv[j] = i->c_str(), argvlen[j] = i->size();
    }

    redisReply *reply = static_cast<redisReply *>(redisCommandArgv(pRedisConn->getCtx(), argv.size(), &(argv[0]), &(argvlen[0])));
    if (RedisPool::CheckReply(reply)) {
        bRet = true;
    }
    else {
        SetErrInfo(dbi, reply);
    }
    RedisPool::FreeReply(reply);
    return bRet;
}

bool xRedisClient::subscribe(const RedisDBIdx& dbi, const KEYS& channels, xRedisContext& ctx) {
    SETDEFAULTIOTYPE(MASTER);
    VDATA vCmdData;
    vCmdData.push_back("SUBSCRIBE");
    addparam(vCmdData, channels);
    return commandargv_array_ex(dbi, vCmdData, ctx);
}

bool xRedisClient::unsubscribe(const RedisDBIdx& dbi, const KEYS& channels, xRedisContext& ctx) {
    SETDEFAULTIOTYPE(MASTER);
    VDATA vCmdData;
    vCmdData.push_back("UNSUBSCRIBE");
    addparam(vCmdData, channels);

    bool bRet = false;
    RedisConn *pRedisConn = static_cast<RedisConn *>(ctx.conn);
    if (NULL == pRedisConn) {
        SetErrString(dbi, GET_CONNECT_ERROR, ::strlen(GET_CONNECT_ERROR));
        return false;
    }

    vector<const char*> argv(vCmdData.size());
    vector<size_t> argvlen(vCmdData.size());
    unsigned int j = 0;
    for (VDATA::const_iterator i = vCmdData.begin(); i != vCmdData.end(); ++i, ++j) {
        argv[j] = i->c_str(), argvlen[j] = i->size();
    }

    redisReply *reply = static_cast<redisReply *>(redisCommandArgv(pRedisConn->getCtx(), argv.size(), &(argv[0]), &(argvlen[0])));
    if (RedisPool::CheckReply(reply)) {
        bRet = true;
    } else {
        SetErrInfo(dbi, reply);
    }
    RedisPool::FreeReply(reply);
    return bRet;
}













