/*
 * ----------------------------------------------------------------------------
 * Copyright (c) 2013-2021, xSky <guozhw at gmail dot com>
 * All rights reserved.
 * Distributed under GPL license.
 * ----------------------------------------------------------------------------
 */

#include "xRedisClusterClient.h"
#include "xLog.h"
#include "xRedisClusterManager.h"

namespace xrc {

static const uint16_t crc16tab[256] = {
    0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50a5, 0x60c6, 0x70e7, 0x8108,
    0x9129, 0xa14a, 0xb16b, 0xc18c, 0xd1ad, 0xe1ce, 0xf1ef, 0x1231, 0x0210,
    0x3273, 0x2252, 0x52b5, 0x4294, 0x72f7, 0x62d6, 0x9339, 0x8318, 0xb37b,
    0xa35a, 0xd3bd, 0xc39c, 0xf3ff, 0xe3de, 0x2462, 0x3443, 0x0420, 0x1401,
    0x64e6, 0x74c7, 0x44a4, 0x5485, 0xa56a, 0xb54b, 0x8528, 0x9509, 0xe5ee,
    0xf5cf, 0xc5ac, 0xd58d, 0x3653, 0x2672, 0x1611, 0x0630, 0x76d7, 0x66f6,
    0x5695, 0x46b4, 0xb75b, 0xa77a, 0x9719, 0x8738, 0xf7df, 0xe7fe, 0xd79d,
    0xc7bc, 0x48c4, 0x58e5, 0x6886, 0x78a7, 0x0840, 0x1861, 0x2802, 0x3823,
    0xc9cc, 0xd9ed, 0xe98e, 0xf9af, 0x8948, 0x9969, 0xa90a, 0xb92b, 0x5af5,
    0x4ad4, 0x7ab7, 0x6a96, 0x1a71, 0x0a50, 0x3a33, 0x2a12, 0xdbfd, 0xcbdc,
    0xfbbf, 0xeb9e, 0x9b79, 0x8b58, 0xbb3b, 0xab1a, 0x6ca6, 0x7c87, 0x4ce4,
    0x5cc5, 0x2c22, 0x3c03, 0x0c60, 0x1c41, 0xedae, 0xfd8f, 0xcdec, 0xddcd,
    0xad2a, 0xbd0b, 0x8d68, 0x9d49, 0x7e97, 0x6eb6, 0x5ed5, 0x4ef4, 0x3e13,
    0x2e32, 0x1e51, 0x0e70, 0xff9f, 0xefbe, 0xdfdd, 0xcffc, 0xbf1b, 0xaf3a,
    0x9f59, 0x8f78, 0x9188, 0x81a9, 0xb1ca, 0xa1eb, 0xd10c, 0xc12d, 0xf14e,
    0xe16f, 0x1080, 0x00a1, 0x30c2, 0x20e3, 0x5004, 0x4025, 0x7046, 0x6067,
    0x83b9, 0x9398, 0xa3fb, 0xb3da, 0xc33d, 0xd31c, 0xe37f, 0xf35e, 0x02b1,
    0x1290, 0x22f3, 0x32d2, 0x4235, 0x5214, 0x6277, 0x7256, 0xb5ea, 0xa5cb,
    0x95a8, 0x8589, 0xf56e, 0xe54f, 0xd52c, 0xc50d, 0x34e2, 0x24c3, 0x14a0,
    0x0481, 0x7466, 0x6447, 0x5424, 0x4405, 0xa7db, 0xb7fa, 0x8799, 0x97b8,
    0xe75f, 0xf77e, 0xc71d, 0xd73c, 0x26d3, 0x36f2, 0x0691, 0x16b0, 0x6657,
    0x7676, 0x4615, 0x5634, 0xd94c, 0xc96d, 0xf90e, 0xe92f, 0x99c8, 0x89e9,
    0xb98a, 0xa9ab, 0x5844, 0x4865, 0x7806, 0x6827, 0x18c0, 0x08e1, 0x3882,
    0x28a3, 0xcb7d, 0xdb5c, 0xeb3f, 0xfb1e, 0x8bf9, 0x9bd8, 0xabbb, 0xbb9a,
    0x4a75, 0x5a54, 0x6a37, 0x7a16, 0x0af1, 0x1ad0, 0x2ab3, 0x3a92, 0xfd2e,
    0xed0f, 0xdd6c, 0xcd4d, 0xbdaa, 0xad8b, 0x9de8, 0x8dc9, 0x7c26, 0x6c07,
    0x5c64, 0x4c45, 0x3ca2, 0x2c83, 0x1ce0, 0x0cc1, 0xef1f, 0xff3e, 0xcf5d,
    0xdf7c, 0xaf9b, 0xbfba, 0x8fd9, 0x9ff8, 0x6e17, 0x7e36, 0x4e55, 0x5e74,
    0x2e93, 0x3eb2, 0x0ed1, 0x1ef0
};

uint16_t xRedisClusterManager::crc16(const char* buf, int32_t len)
{
    int32_t counter;
    uint16_t crc = 0;
    for (counter = 0; counter < len; counter++)
        crc = (crc << 8) ^ crc16tab[((crc >> 8) ^ *buf++) & 0x00FF];
    return crc;
}

bool xRedisClusterManager::checkReply(const redisReply* reply)
{
    if (NULL == reply) {
        return false;
    }

    switch (reply->type) {
    case REDIS_REPLY_STRING: {
        return true;
    }
    case REDIS_REPLY_ARRAY: {
        return true;
    }
    case REDIS_REPLY_INTEGER: {
        return true;
    }
    case REDIS_REPLY_NIL: {
        return false;
    }
    case REDIS_REPLY_STATUS: {
        return true;
    }
    case REDIS_REPLY_ERROR: {
        return false;
    }
    default: {
        return false;
    }
    }

    return false;
}

void xRedisClusterManager::freeReply(const redisReply* reply)
{
    if (NULL != reply) {
        freeReplyObject((void*)reply);
    }
}

int32_t xRedisClusterManager::str2Vect(const char* pSrc,
    std::vector<std::string>& vDest,
    const char* pSep)
{
    if (NULL == pSrc) {
        return -1;
    }

    int32_t iLen = strlen(pSrc);
    if (iLen == 0) {
        return -1;
    }

    char* pTmp = new char[iLen + 1];
    if (pTmp == NULL) {
        return -1;
    }

    memcpy(pTmp, pSrc, iLen);
    pTmp[iLen] = '\0';

    char* pCurr = strtok(pTmp, pSep);
    while (pCurr) {
        vDest.push_back(pCurr);
        pCurr = strtok(NULL, pSep);
    }

    delete[] pTmp;
    return 0;
}

xRedisClusterManager::xRedisClusterManager()
{
    mRedisConnList = NULL;
    mLock = NULL;
    bFree = false;
}

xRedisClusterManager::~xRedisClusterManager() { release(); }

void xRedisClusterManager::keepalive()
{
    xredis_debug("mPoolSize:%u vNodes.size:%u clusterEnabled:%u",
        mClusterInfo.poolsize, mClusterInfo.nodes.size(), mClusterInfo.clusterEnabled);

    size_t node_count = mClusterInfo.nodes.size();
    for (size_t i = 0; i < node_count; ++i) {
        XLOCK(mLock[i]);
        RedisConnectionList* pList = &mRedisConnList[i];
        RedisConnectionIter iter = pList->begin();

        if (mClusterInfo.poolsize != pList->size()) {
            xredis_warn("mPoolSize:%u pList.size:%u", mClusterInfo.poolsize, pList->size());
        }

        for (; iter != pList->end(); iter++) {
            RedisConnection* pConn = *iter;
            if (!pConn->ping()) {
                xredis_error("ping error index:%u %s:%u mPoolSize:%u vNodes.size:%u clusterEnabled:%u",
                    i, pConn->mHost.c_str(), pConn->mPort, mClusterInfo.poolsize, mClusterInfo.nodes.size(), mClusterInfo.clusterEnabled);
                if (pConn->redisReConnect()) {
                    xredis_debug("RedisReConnect success index:%u %s:%u mPoolSize:%u vNodes.size:%u clusterEnabled:%u",
                        i, pConn->mHost.c_str(), pConn->mPort, mClusterInfo.poolsize, mClusterInfo.nodes.size(), mClusterInfo.clusterEnabled);
                } else {
                    xredis_error("RedisReConnect error index:%u %s:%u  mPoolSize:%u vNodes.size:%u clusterEnabled:%u",
                        i, pConn->mHost.c_str(), pConn->mPort, mClusterInfo.poolsize, mClusterInfo.nodes.size(), mClusterInfo.clusterEnabled);
                }
            } else {
                xredis_debug("ping ok index:%u %s:%u  mPoolSize:%u vNodes.size:%u clusterEnabled:%u",
                    i, pConn->mHost.c_str(), pConn->mPort, mClusterInfo.poolsize, mClusterInfo.nodes.size(), mClusterInfo.clusterEnabled);
            }
        }
    }
}

bool xRedisClusterManager::release()
{
    xredis_debug("nodes.size:%u poolsize:%u", mClusterInfo.nodes.size(), mClusterInfo.poolsize);
    NODELIST& vNodes = mClusterInfo.nodes;

    size_t node_count = vNodes.size();
    for (size_t i = 0; i < node_count; ++i) {
        RedisConnectionList* pList = &mRedisConnList[i];
        if (pList->size() != mClusterInfo.poolsize) {
            xredis_error("pList.size:%u poolsize:%u", pList->size(), mClusterInfo.poolsize);
            return false;
        }
    }

    xredis_info("所有的连接都回来了：%u", mClusterInfo.poolsize);

    for (size_t i = 0; i < node_count; ++i) {
        RedisConnectionList* pList = &mRedisConnList[i];
        RedisConnectionIter iter = pList->begin();
        for (; iter != pList->end(); iter++) {
            xredis_debug("index:%u %s:%u poolsize:%u ", (*iter)->mIndex, (*iter)->mHost.c_str(), (*iter)->mPort, (*iter)->mPoolSize);
            redisFree((*iter)->mCtx);
            delete *iter;
        }
    }

    vNodes.clear();
    delete[] mRedisConnList;
    mRedisConnList = NULL;
    delete[] mLock;
    mLock = NULL;

    return true;
}

bool xRedisClusterManager::checkReply(redisReply* reply)
{
    if (NULL == reply) {
        return false;
    }
    return true;
}

bool xRedisClusterManager::clusterEnabled(redisContext* ctx)
{
    redisReply* redis_reply = (redisReply*)redisCommand(ctx, "info");
    if ((NULL == redis_reply) || (NULL == redis_reply->str)) {
        if (redis_reply) {
            freeReplyObject(redis_reply);
        }
        return false;
    }

    char* p = strstr(redis_reply->str, "cluster_enabled:");
    bool bRet = p != NULL && (0 == strncmp(p + strlen("cluster_enabled:"), "1", 1));
    freeReplyObject(redis_reply);
    return bRet;
}

bool xRedisClusterManager::clusterState(redisContext* ctx)
{
    redisReply* redis_reply = (redisReply*)redisCommand(ctx, "CLUSTER info");
    if ((NULL == redis_reply) || (NULL == redis_reply->str)) {
        if (redis_reply) {
            freeReplyObject(redis_reply);
        }
        return false;
    }
    char* p = strstr(redis_reply->str, ":");
    bool bRet = (0 == strncmp(p + 1, "ok", 2));
    freeReplyObject(redis_reply);
    return bRet;
}

bool xRedisClusterManager::clusterNodes(redisContext* redis_ctx, ClusterInfo* info)
{
    info->nodes.clear();
    std::vector<std::string> vlines;
    redisReply* redis_reply = (redisReply*)redisCommand(redis_ctx, "CLUSTER NODES");
    if ((NULL == redis_reply) || (NULL == redis_reply->str)) {
        if (redis_reply) {
            freeReplyObject(redis_reply);
        }
        redisFree(redis_ctx);
        return false;
    }

    xredis_debug("\r\n%s", redis_reply->str);

    str2Vect(redis_reply->str, vlines, "\n");

    for (size_t i = 0; i < vlines.size(); ++i) {
        NodeInfo node;
        node.strinfo = vlines[i];

        std::vector<std::string> nodeinfo;
        str2Vect(node.strinfo.c_str(), nodeinfo, " ");
        xredis_debug("nodeinfo.size: %u", nodeinfo.size());

        for (size_t k = 0; k < nodeinfo.size(); ++k) {
            //xredis_debug("%lu : %s \r\n", k, nodeinfo[k].c_str());
        }

        if (NULL != strstr(nodeinfo[7].c_str(), "disconnected")) {
            xredis_warn("id:%s %s disconnected", nodeinfo[0].c_str(), nodeinfo[1].c_str());
            continue;
        }
        if (NULL == strstr(nodeinfo[2].c_str(), "master")) {
            xredis_debug("%s \r\n", nodeinfo[2].c_str());
            continue;
        }
        node.id = nodeinfo[0];
        node.parse_host(nodeinfo[1]);
        node.flags = nodeinfo[2];
        node.parse_role(nodeinfo[3]);
        node.master_id = nodeinfo[4];
        node.ping_sent = (NULL == nodeinfo[5].c_str()) ? (0) : ((uint32_t)atol(nodeinfo[5].c_str()));
        node.pong_recv = (NULL == nodeinfo[6].c_str()) ? (0) : ((uint32_t)atol(nodeinfo[6].c_str()));
        node.epoch = (NULL == nodeinfo[7].c_str()) ? (0) : ((uint32_t)atol(nodeinfo[7].c_str()));

        if (nodeinfo.size() > 8) {
            for (uint32_t i = 0; i < nodeinfo.size() - 8; i++) {
                node.parse_slot(nodeinfo[8 + i]);
            }
        } else {
            xredis_debug("这个节点没有数据solt分配 id:%s %s:%u ", node.id.c_str(), node.ip.c_str(), node.port);
        }

        info->nodes.push_back(node);
    }

    xredis_debug("nodes.size:%u", info->nodes.size());

    return true;
}

bool xRedisClusterManager::auth(redisContext* c, const std::string& pass)
{
    bool bRet = false;
    if (0 == pass.length()) {
        bRet = true;
    } else {
        redisReply* reply = static_cast<redisReply*>(redisCommand(c, "AUTH %s", pass.c_str()));
        if ((NULL == reply) || (strcasecmp(reply->str, "OK") != 0)) {
            bRet = false;
        } else {
            bRet = true;
        }
        freeReplyObject(reply);
    }

    return bRet;
}

bool xRedisClusterManager::connectCluster(const ClusterInfo* cluster_info)
{
    mClusterInfo.nodes = cluster_info->nodes;
    mClusterInfo.pass = cluster_info->pass;
    mClusterInfo.poolsize = cluster_info->poolsize;
    mClusterInfo.clusterEnabled = cluster_info->clusterEnabled;
    NODELIST& vNodes = mClusterInfo.nodes;

    for (size_t i = 0; i < mClusterInfo.nodes.size(); ++i) {
        NodeInfo* pNode = &mClusterInfo.nodes[i];
        xredis_debug(" %s %s:%u %u ", pNode->id.c_str(), pNode->ip.c_str(), pNode->port, pNode->is_master);
    }

    xredis_debug("connectCluster nodes:%u pass:%s poolsize:%u",
        cluster_info->nodes.size(), cluster_info->pass.c_str(), cluster_info->poolsize);

    int32_t cnt = vNodes.size();
    mRedisConnList = new RedisConnectionList[cnt];
    mLock = new xLock[cnt];
    for (int32_t i = 0; i < cnt; ++i) {
        connectRedisNode(i, vNodes[i].ip.c_str(), vNodes[i].port, mClusterInfo.pass, mClusterInfo.poolsize);
    }

    return true;
}

bool xRedisClusterManager::connectRedisNode(int32_t idx, const std::string& host,
    uint32_t port,
    const std::string& pass,
    uint32_t poolsize)
{
    if (0 == host.length()) {
        xredis_error("host error \n");
        return false;
    }

    //同时打开 CONNECTION_NUM 个连接
    poolsize = poolsize > MAX_REDIS_POOLSIZE ? MAX_REDIS_POOLSIZE : poolsize;

    xredis_info("connectRedisNode host:%s port:%u pass:%s poolsize:%u \n",
        host.c_str(), port, pass.c_str(), poolsize);

    for (uint32_t i = 0; i < poolsize; ++i) {
        struct timeval timeoutVal;
        timeoutVal.tv_sec = MAX_TIME_OUT;
        timeoutVal.tv_usec = 0;

        RedisConnection* pRedisconn = new RedisConnection;
        if (NULL == pRedisconn) {
            xredis_error("ConnectRedisNode host:%s port:%u pass:%s poolsize:%u \n",
                host.c_str(), port, pass.c_str(), poolsize);
            continue;
        }
        pRedisconn->mHost = host;
        pRedisconn->mPass = pass;
        pRedisconn->mPort = port;
        pRedisconn->mPoolSize = poolsize;
        pRedisconn->mIndex = idx;
        pRedisconn->mCtx = redisConnectWithTimeout(host.c_str(), port, timeoutVal);
        if (pRedisconn->mCtx == NULL || pRedisconn->mCtx->err) {
            if (pRedisconn->mCtx) {
                redisFree(pRedisconn->mCtx);
                xredis_error("redisConnectWithTimeout error host:%s port:%u pass:%s poolsize:%u \n",
                    host.c_str(), port, pass.c_str(), poolsize);
            } else {
                xredis_warn("redisConnectWithTimeout error idx:%u host:%s port:%u pass:%s poolsize:%u \n",
                    idx, host.c_str(), port, pass.c_str(), poolsize);
            }
            delete pRedisconn;
            return false;
        } else {
            if (!auth(pRedisconn->mCtx, pass)) {
                xredis_error("auth error idx:%u host:%s port:%u pass:%s poolsize:%u \n",
                    idx, host.c_str(), port, pass.c_str(), poolsize);

                delete pRedisconn;
                return false;
            }
            xredis_debug("auth success idx:%u host:%s port:%u pass:%s poolsize:%u \n",
                idx, host.c_str(), port, pass.c_str(), poolsize);
            mRedisConnList[idx].push_back(pRedisconn);
        }
    }

    xredis_info("connectRedisNode success idx:%u host:%s port:%u pass:%s poolsize:%u \n",
        idx, host.c_str(), port, pass.c_str(), poolsize);

    return true;
}

RedisConnection* xRedisClusterManager::getConnection(uint32_t idx)
{
    RedisConnection* pRedisConn = NULL;

    while (1) {
        {
            XLOCK(mLock[idx]);
            if (mRedisConnList[idx].size() > 0) {
                pRedisConn = mRedisConnList[idx].front();
                mRedisConnList[idx].pop_front();
                break;
            } else {
                xredis_error("RedisPool::getConnection()  error pthread_id=%lu \n",
                    pthread_self());
            }
        }
        usleep(100);
    }

    xredis_debug("index:%u size:%u", pRedisConn->mIndex, mRedisConnList[idx].size());

    return pRedisConn;
}

void xRedisClusterManager::freeConnection(RedisConnection* pRedisConn)
{
    xredis_debug("index:%u size:%u", pRedisConn->mIndex, mRedisConnList[pRedisConn->mIndex].size());

    XLOCK(mLock[pRedisConn->mIndex]);
    mRedisConnList[pRedisConn->mIndex].push_back(pRedisConn);
}

/* Copy from cluster.c
 * We have 16384 hash slots. The hash slot of a given key is obtained
 * as the least significant 14 bits of the crc16 of the key.
 *
 * However if the key contains the {...} pattern, only the part between
 * { and } is hashed. This may be useful in the future to force certain
 * keys to be in the same node (assuming no resharding is in progress). */
uint32_t xRedisClusterManager::keyHashSlot(const char* key, size_t keylen)
{
    size_t s, e; /* start-end indexes of { and } */

    for (s = 0; s < keylen; s++)
        if (key[s] == '{')
            break;

    /* No '{' ? Hash the whole key. This is the base case. */
    if (s == keylen)
        return crc16(key, keylen) & 0x3FFF;

    /* '{' found? Check if we have the corresponding '}'. */
    for (e = s + 1; e < keylen; e++)
        if (key[e] == '}')
            break;

    /* No '}' or nothing betweeen {} ? Hash the whole key. */
    if (e == keylen || e == s + 1)
        return crc16(key, keylen) & 0x3FFF;

    /* If we are here there is both a { and a } on its right. Hash
   * what is in the middle between { and }. */
    return crc16(key + s + 1, e - s - 1) & 0x3FFF; // 0x3FFF == 16383
}

uint32_t xRedisClusterManager::findNodeIndex(uint32_t slot)
{
    for (size_t i = 0; i < mClusterInfo.nodes.size(); ++i) {
        NodeInfo* pNode = &mClusterInfo.nodes[i];
        if (pNode->checkSlot(slot)) {
            return i;
        }
    }
    return 0;
}

uint32_t xRedisClusterManager::getKeySlotIndex(const char* key)
{
    if (NULL != key) {
        return keyHashSlot(key, strlen(key));
    }
    return 0;
}

RedisConnection* xRedisClusterManager::findNodeConnection(const char* key)
{
    if (!mClusterInfo.clusterEnabled) {
        xredis_debug("mClusterInfo.clusterEnabled: %u %s", mClusterInfo.clusterEnabled, key);
        return getConnection(0);
    }
    uint32_t slot_id = getKeySlotIndex(key);
    uint32_t index = findNodeIndex(slot_id);

    xredis_debug("slot_id:%u index:%u %s:%u ", slot_id, index,
        mClusterInfo.nodes[index].ip.c_str(), mClusterInfo.nodes[index].port);

    return getConnection(index);
}

bool xRedisClusterManager::command(RedisResult& result, const char* format, const char* key, va_list args)
{
    bool bRet = false;
    RedisConnection* pRedisConn = NULL;
    pRedisConn = findNodeConnection(key);

    redisReply* reply = static_cast<redisReply*>(redisvCommand(pRedisConn->mCtx, format, args));
    if (checkReply(reply)) {
        result.Init(reply);
        bRet = true;
    } else {
        bRet = false;
    }

    freeConnection(pRedisConn);
    return bRet;
}

bool xRedisClusterManager::commandArgv(const VSTRING& vDataIn,
    RedisResult& result)
{
    bool bRet = false;
    const std::string& key = vDataIn[1];
    RedisConnection* pRedisConn = findNodeConnection(key.c_str());
    if (NULL == pRedisConn) {
        return false;
    }

    std::vector<const char*> argv(vDataIn.size());
    std::vector<size_t> argvlen(vDataIn.size());
    unsigned int j = 0;
    for (VSTRING::const_iterator i = vDataIn.begin(); i != vDataIn.end();
         ++i, ++j) {
        argv[j] = i->c_str(), argvlen[j] = i->size();
    }

    redisReply* reply = static_cast<redisReply*>(redisCommandArgv(
        pRedisConn->mCtx, argv.size(), &(argv[0]), &(argvlen[0])));
    if (xRedisClusterManager::checkReply(reply)) {
        result.Init(reply);
        bRet = true;
    } else {
        // SetErrInfo(dbi, reply);
    }

    freeConnection(pRedisConn);
    return bRet;
}

RedisConnection* xrc::xRedisClusterManager::get_cluster_info(ClusterInfo& info)
{
    RedisConnection* pConn = NULL;
    if (!info.clusterEnabled) {
        return pConn;
    }

    // 从当前集群中取一个可用连接，查询集群信息
    uint32_t nodes_count = mClusterInfo.nodes.size();
    for (uint32_t i = 0; i < nodes_count; ++i) {
        pConn = getConnection(i);
        if (NULL == pConn) {
            xredis_warn("GetConnection error index:%u nodes_count:%u \n", i, nodes_count);
            continue;
        }
        if (clusterNodes(pConn->mCtx, &info)) {
            xredis_debug("get cluster nodes OK size:%u \n", info.nodes.size());
            break;
        }
    }

    return pConn;
}

// 有变化时需要重建立集群连接 返回 false
// 无变化，返回true
bool xrc::xRedisClusterManager::check_cluster_info(ClusterInfo& cinfo)
{
    NODELIST node_cur;
    node_cur.assign(cinfo.nodes.begin(), cinfo.nodes.end());
    std::sort(node_cur.begin(), node_cur.end(), Sort_asc);

    NODELIST node_old;
    node_old.assign(mClusterInfo.nodes.begin(), mClusterInfo.nodes.end());
    std::sort(node_old.begin(), node_old.end(), Sort_asc);

    if (node_cur.size() != node_old.size()) {
        xredis_error("node_cur size:%u node_old size:%u \n",
            node_cur.size(), node_old.size());
        return false;
    }

    for (uint32_t i = 0; i < node_cur.size(); ++i) {
        xredis_debug("%u node_cur id:%s node_old id:%s \n", i, node_cur[i].id.c_str(), node_old[i].id.c_str());
        NodeInfo& info_cur = node_cur[i];
        NodeInfo& info_old = node_old[i];

        if (info_cur.id != info_old.id) {
            xredis_warn("%u info_cur id:%s info_old id:%s \n", info_cur.id.c_str(), info_old.id.c_str());
            return false;
        }

        if (info_cur.ip != info_old.ip) {
            xredis_warn("%u info_cur ip:%s info_old ip:%s \n", info_cur.ip.c_str(), info_old.ip.c_str());
            return false;
        }

        if (info_cur.port != info_old.port) {
            xredis_warn("%u info_cur port:%u info_old port:%u \n", info_cur.port, info_old.port);
            return false;
        }

        if (info_cur.connected != info_old.connected) {
            xredis_warn("%u info_cur connected:%u info_old connected:%u \n", info_cur.connected, info_old.connected);
            return false;
        }

        if (info_cur.mSlots != info_old.mSlots) {
            xredis_warn("index:%u info_cur mSlots:%u info_old mSlots:%u \n", i, info_cur.mSlots.size(), info_old.mSlots.size());

            for (uint32_t i = 0; i < info_cur.mSlots.size(); ++i) {
                std::pair<uint32_t, uint32_t>& iter_cur = info_cur.mSlots[i];
                std::pair<uint32_t, uint32_t>& iter_old = info_old.mSlots[i];

                xredis_debug("check %u [%u, %u] : [%u, %u]\n", i,
                    iter_cur.first, iter_cur.second,
                    iter_old.first, iter_old.second);
            }

            return false;
        }
    }

    xredis_debug("the redis cluster state has not changed");

    return true;
}

xRedisClusterManager* xrc::xRedisClusterManager::connectRedis(const std::string& host, uint32_t port,
    const std::string& pass, uint32_t poolsize,  ClusterInfo* &info)
{
    xredis_info("host:%s port:%u pass:%s poolsize:%u", host.c_str(), port, pass.c_str(), poolsize);

    if (NULL == info) {
        info = new ClusterInfo;
    }

    struct timeval timeoutVal;
    timeoutVal.tv_sec = 5;
    timeoutVal.tv_usec = 0;

    info->pass = pass;
    info->poolsize = poolsize;

    redisContext* redis_ctx = redisConnectWithTimeout(host.c_str(), port, timeoutVal);
    if (redis_ctx == NULL || redis_ctx->err) {
        if (redis_ctx) {
            redisFree(redis_ctx);
        } else {
            xredis_error("Connection error: can't allocate redis context, %s  \n",
                redis_ctx->errstr);
        }
        xredis_error("redisConnectWithTimeout error \n");
        return NULL;
    } else {
        if (!xRedisClusterManager::auth(redis_ctx, pass)) {
            xredis_error("auth error \n");
            return NULL;
        }
    }

    info->clusterEnabled = xRedisClusterManager::clusterEnabled(redis_ctx);
    xredis_info("connectRedis clusterEnabled:%u \n", info->clusterEnabled);

    if (!info->clusterEnabled) {
        // 如果没有启用集群模式，则使用单节点redis
        NodeInfo node;
        node.ip = host;
        node.port = port;
        node.is_master = true;
        node.connected = true;

        info->poolsize = poolsize;
        info->pass = pass;
        info->nodes.push_back(node);
        xredis_info("Using single redis, size:%u poolsize:%u", info->nodes.size(), info->poolsize);
        redisFree(redis_ctx);
    } else {
        // 查询redis集群节点列表
        if (!xRedisClusterManager::clusterNodes(redis_ctx, info)) {
            xredis_error("clusterNodes error \n");
            redisFree(redis_ctx);
            return NULL;
        }
        xredis_debug("clusterNodes nodes.size:%u \n", info->nodes.size());

        // 判断下集群状态，
        if (!xRedisClusterManager::clusterState(redis_ctx)) {
            xredis_error("clusterState error \n");
            redisFree(redis_ctx);
            return NULL;
        }
        xredis_debug("cluster_state is OK \n");

        redisFree(redis_ctx);
    }

    xRedisClusterManager* pClusterManager = new xRedisClusterManager;
    if (!pClusterManager->connectCluster(info)) {
        xredis_error("ConnectCluster error \n");
        return NULL;
    }

    xredis_info("Connect to redis cluster sucess \n");

    return pClusterManager;
}


xrc::xRedisClusterClient::xRedisClusterClient()
{
    mClusterManager = NULL;
    mClusterManager_free = NULL;
    mRedisInfo = NULL;
}

xrc::xRedisClusterClient::~xRedisClusterClient()
{
    if (NULL != mClusterManager) {
        delete mClusterManager;
        mClusterManager = NULL;
    }

    if (NULL != mClusterManager_free) {
        delete mClusterManager_free;
        mClusterManager_free = NULL;
    }

    if (NULL != mRedisInfo) {
        delete mRedisInfo;
        mRedisInfo = NULL;
    }
}

bool xrc::xRedisClusterClient::connect(const std::string& host, uint32_t port, const std::string& pass, uint32_t poolsize)
{
    return (mClusterManager = xRedisClusterManager::connectRedis(host, port, pass, poolsize, mRedisInfo));
}

void xrc::xRedisClusterClient::keepalive()
{
    if (NULL != mClusterManager_free) {
        if (mClusterManager_free->release()) {
            delete mClusterManager_free;
            mClusterManager_free = NULL;
            xredis_warn("mClusterManager_free release OK ");
        } else {
            xredis_warn("Having problems with cluster connection collection?");
        }
    }

    if (mRedisInfo->clusterEnabled) {
        RedisConnection* pConn = mClusterManager->get_cluster_info(*mRedisInfo);
        if (NULL != pConn) {
            if (!mClusterManager->check_cluster_info(*mRedisInfo)) {
                xredis_warn("The cluster state has changed , the connection pool needs to be reestablished");
                xRedisClusterManager* pClusterManagerNew = xRedisClusterManager::connectRedis(pConn->mHost, pConn->mPort, pConn->mPass, pConn->mPoolSize, mRedisInfo);
                mClusterManager->freeConnection(pConn);
                mClusterManager->bfree();

                mClusterManager_free = mClusterManager;
                xredis_info("Switch the connection ClusterManager pClusterManagerNew:%p mClusterManager:%p ", pClusterManagerNew, mClusterManager);
                mClusterManager = pClusterManagerNew;
                return;
            }
            mClusterManager->freeConnection(pConn);
        } else {
            xredis_error("Maybe the cluster nodes are all down?");
        }
    }

    mClusterManager->keepalive();
}

bool xrc::xRedisClusterClient::commandArgv(const VSTRING& vDataIn, RedisResult& result)
{
    return mClusterManager->commandArgv(vDataIn, result);
}

bool xrc::xRedisClusterClient::command(RedisResult& result, const char* format, ...)
{
    va_list ap;
    va_start(ap, format);
    char* key = va_arg(ap, char*);
    if (0 == strlen(key)) {
        va_end(ap);
        return false;
    }

    va_start(ap, format);
    bool ret = mClusterManager->command(result, format, key, ap);
    va_end(ap);

    return ret;
}

void xrc::xRedisClusterClient::setLogLevel(uint32_t level, void (*emit)(int level, const char* line))
{
    set_log_level(level, emit);
}

} // namespace xrc
