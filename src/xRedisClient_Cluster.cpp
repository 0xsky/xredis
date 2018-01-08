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


RedisDBICluster::RedisDBICluster()
{
    uCount = 0;
}

RedisDBICluster::~RedisDBICluster()
{
}

int RedisDBICluster::str2Vect(const char* pSrc, vector<string> &vDest, const char *pSep) {
    if (NULL == pSrc) {
        return -1;
    }

    int iLen = strlen(pSrc);
    if (iLen == 0) {
        return -1;
    }

    char *pTmp = new char[iLen + 1];
    if (pTmp == NULL) {
        return -1;
    }

    memcpy(pTmp, pSrc, iLen);
    pTmp[iLen] = '\0';

    char *pCurr = strtok(pTmp, pSep);
    while (pCurr) {
        vDest.push_back(pCurr);
        pCurr = strtok(NULL, pSep);
    }

    delete[] pTmp;
    return 0;
}

bool RedisDBICluster::ClusterEnabled(redisContext *ctx)
{
    redisReply *redis_reply = (redisReply*)redisCommand(ctx, "info");
    if ((NULL == redis_reply) || (NULL == redis_reply->str)) {
        if (redis_reply) {
            freeReplyObject(redis_reply);
        }
        return false;
    }
    //printf("redis info:\r\n%s\r\n", redis_reply->str);
    char *p = strstr(redis_reply->str, "cluster_enabled:");
    bool bRet = p != NULL && (0 == strncmp(p + strlen("cluster_enabled:"), "1", 1));
    //printf("--:%s\r\n", p + strlen("cluster_enabled:"));
    freeReplyObject(redis_reply);
    return bRet;
}

bool RedisDBICluster::Clusterinfo(redisContext *ctx)
{
    redisReply *redis_reply = (redisReply*)redisCommand(ctx, "CLUSTER info");
    if ((NULL == redis_reply) || (NULL == redis_reply->str)) {
        if (redis_reply) {
            freeReplyObject(redis_reply);
        }
        return false;
    }
    //printf("Clusterinfo:\r\n%s\r\n", redis_reply->str);
    char *p = strstr(redis_reply->str, ":");
    bool bRet = (0 == strncmp(p + 1, "ok", 2));
    freeReplyObject(redis_reply);
    return bRet;
}

bool RedisDBICluster::ConnectRedisCluster(const char *host, uint32_t port, uint32_t poolsize)
{
    if (NULL == host) {
        printf("error argv \r\n");
        return false;
    }

    struct timeval timeoutVal;
    timeoutVal.tv_sec = 5;
    timeoutVal.tv_usec = 0;
    //mPoolSize = poolsize;

    redisContext *redis_ctx = redisConnectWithTimeout(host, port, timeoutVal);
    if (redis_ctx == NULL || redis_ctx->err) {
        if (redis_ctx) {
            printf("Connection error: %s \r\n", redis_ctx->errstr);
            redisFree(redis_ctx);
            return false;
        }
        else {
            printf("Connection error: can't allocate redis context, %s  \n", redis_ctx->errstr);
        }
    }
    else {
        printf("Connect to Redis: %s:%d  success \n", host, port);
    }

    bool mClusterEnabled = ClusterEnabled(redis_ctx);
    printf("ClusterEnabled %d \r\n", mClusterEnabled);

    //if (!mClusterEnabled) {
    //    mRedisConnList = new RedisConnectionList[1];
    //    ConnectRedisNode(0, host, port, mPoolSize);
    //    return true;
    //}

    if (!Clusterinfo(redis_ctx)) {
        printf("Clusterinfo error \r\n");
        return false;
    }

    vector<string> vlines;
    redisReply *redis_reply = (redisReply*)redisCommand(redis_ctx, "CLUSTER NODES");
    if ((NULL == redis_reply) || (NULL == redis_reply->str)) {
        if (redis_reply) {
            freeReplyObject(redis_reply);
        }
        redisFree(redis_ctx);
        return false;
    }

    str2Vect(redis_reply->str, vlines, "\n");
    printf("vlines:%lu\r\n", vlines.size());

    pRedisNodeList = new RedisNode[vlines.size()];
    for (size_t i = 0; i < vlines.size(); ++i) {
        NodeInfo node;
        node.strinfo = vlines[i];

        vector<string> nodeinfo;
        str2Vect(node.strinfo.c_str(), nodeinfo, " ");
        for (size_t k = 0; k < nodeinfo.size(); ++k) {
            printf("%lu : %s \r\n", k, nodeinfo[k].c_str());
        }
        if (NULL == strstr(nodeinfo[2].c_str(), "master")) {
            printf("%s \r\n", nodeinfo[2].c_str());
            continue;
        }
        node.id = nodeinfo[0];
        node.ParseNodeString(nodeinfo[1]);
        node.ParseSlotString(nodeinfo[8]);
        printf("------------------------\r\n");
        vNodes.push_back(node);
        pRedisNodeList[uCount].dbindex = uCount;
        pRedisNodeList[uCount].host = strdup(node.ip.c_str());
        pRedisNodeList[uCount].port = node.port;
        uCount++;
    }

    freeReplyObject(redis_reply);
    redisFree(redis_ctx);

    return true;
}

uint32_t RedisDBICluster::FindNodeIndex(uint32_t slot)
{
    for (size_t i = 0; i < vNodes.size(); ++i) {
        NodeInfo *pNode = &vNodes[i];
        if (pNode->CheckSlot(slot)) {
            printf("FindNode %u:%lu\n", slot, i);
            return i;
        }
    }
    return 0;
}

/* Copy from cluster.c
* We have 16384 hash slots. The hash slot of a given key is obtained
* as the least significant 14 bits of the crc16 of the key.
*
* However if the key contains the {...} pattern, only the part between
* { and } is hashed. This may be useful in the future to force certain
* keys to be in the same node (assuming no resharding is in progress). */
uint32_t RedisDBICluster::KeyHashSlot(const char *key, size_t keylen) {
    size_t s, e; /* start-end indexes of { and } */

    for (s = 0; s < keylen; s++)
        if (key[s] == '{') break;

    /* No '{' ? Hash the whole key. This is the base case. */
    if (s == keylen) return crc16(key, keylen) & 0x3FFF;

    /* '{' found? Check if we have the corresponding '}'. */
    for (e = s + 1; e < keylen; e++)
        if (key[e] == '}') break;

    /* No '}' or nothing betweeen {} ? Hash the whole key. */
    if (e == keylen || e == s + 1) return crc16(key, keylen) & 0x3FFF;

    /* If we are here there is both a { and a } on its right. Hash
    * what is in the middle between { and }. */
    return crc16(key + s + 1, e - s - 1) & 0x3FFF; // 0x3FFF == 16383
}

uint32_t RedisDBICluster::GetKeySlotIndex(const char* key)
{
    if (NULL != key) {
        return KeyHashSlot(key, strlen(key));
    }
    return 0;
}

uint32_t RedisDBICluster::GetClusterIndex(const char *key)
{
    printf("key:%s \r\n", key);
    uint32_t slot_id = GetKeySlotIndex(key);
    uint32_t index = FindNodeIndex(slot_id);
    return index;
}

bool RedisDBICluster::ClusterDBI(const char *key, unsigned int type) {
    unsigned int index = GetClusterIndex(key);
    setIndex(index);
    setType(type);
    return true;
}

