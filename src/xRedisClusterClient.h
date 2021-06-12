/*
 * ----------------------------------------------------------------------------
 * Copyright (c) 2013-2021, xSky <guozhw at gmail dot com>
 * All rights reserved.
 * Distributed under GPL license.
 * ----------------------------------------------------------------------------
 */

#ifndef _XREDIS_XREDISCLUSTERCLIENT_H_
#define _XREDIS_XREDISCLUSTERCLIENT_H_

#include "hiredis.h"
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <vector>

namespace xrc {

#define LOG_LEVEL_ERROR 0
#define LOG_LEVEL_WARN 1
#define LOG_LEVEL_INFO 2
#define LOG_LEVEL_DEBUG 3

class xRedisClusterManager;
class RedisResult;
typedef std::vector<std::string> VString;
class ClusterInfo;

class RedisResult {
public:
    RedisResult() { }
    ~RedisResult()
    {
        if (Reply.reply) {
            freeReplyObject((void*)Reply.reply);
        }
    }

public:
    class RedisReply {
    public:
        RedisReply(redisReply* r) { reply = r; }
        RedisReply(const RedisReply& r) { reply = r.reply; }
        RedisReply() { reply = NULL; }
        ~RedisReply() { }

        int32_t type() const { return reply->type; }
        long long integer() const { return reply->integer; }
        int32_t len() const { return reply->len; }
        char* str() const { return reply->str; }
        size_t elements() const { return reply->elements; }
        struct RedisReply element(uint32_t index) const
        {
            return RedisReply(reply->element[index]);
        }

    private:
        friend class RedisResult;
        redisReply* reply;
    };

    void Init(redisReply* r)
    {
        if (Reply.reply) {
            freeReplyObject((void*)Reply.reply);
        }
        Reply.reply = r;
    }
    int32_t type() const { return Reply.type(); }
    long long integer() const { return Reply.integer(); }
    int32_t len() const { return Reply.len(); }
    char* str() const { return Reply.str(); }
    size_t elements() const { return Reply.elements(); }
    RedisReply element(uint32_t index) const { return Reply.element(index); }

private:
    RedisReply Reply;
};

class xRedisClusterClient {
public:
    xRedisClusterClient();
    ~xRedisClusterClient();

public:
    bool connect(const std::string& host, uint32_t port,
        const std::string& pass, uint32_t poolsize);
    void keepalive();
    bool commandArgv(const VString& vDataIn, RedisResult& result);
    bool command(RedisResult& result, const char* format, ...);
    void setLogLevel(uint32_t level, void (*emit)(int level, const char* line));

private:
    xRedisClusterManager* connectRedis(const std::string& host, uint32_t port,
        const std::string& pass, uint32_t poolsize);

private:
    xRedisClusterManager* mClusterManager_free;
    xRedisClusterManager* mClusterManager;
    ClusterInfo* mRedisInfo;
};

} // namespace xrc

#endif
