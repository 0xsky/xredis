
#ifndef _XREDIS_CLUSTER_MANAGER_H_
#define _XREDIS_CLUSTER_MANAGER_H_

#include "hiredis.h"
#include "xLock.h"
#include "xRedisLog.h"

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include <string>
#include <vector>
#include <list>

namespace xrc {

#define MAX_REDIS_POOLSIZE 128
#define MAX_TIME_OUT 5

typedef std::vector<std::string> VSTRING;

class RedisResult;
typedef struct _REDISCONN_ {
    _REDISCONN_()
    {
        mCtx = NULL;
        mPort = 0;
        mPoolSize = 0;
    }
    ~_REDISCONN_() { }

    redisContext* connectWithTimeout()
    {
        struct timeval timeoutVal;
        timeoutVal.tv_sec = MAX_TIME_OUT;
        timeoutVal.tv_usec = 0;

        redisContext* ctx = NULL;
        ctx = redisConnectWithTimeout(mHost.c_str(), mPort, timeoutVal);
        if (NULL == ctx || ctx->err) {
            if (NULL != ctx) {
                redisFree(ctx);
                ctx = NULL;
            } else {
            }
        }

        return ctx;
    }
    bool ping()
    {
        redisReply* reply = static_cast<redisReply*>(redisCommand(mCtx, "PING"));
        bool bRet = (NULL != reply) && (reply->str) && (strcasecmp(reply->str, "PONG") == 0);
        if (bRet) {
            freeReplyObject(reply);
        }
        return bRet;
    }
    bool redisReConnect()
    {
        bool bRet = false;
        redisContext* tmp_ctx = connectWithTimeout();
        if (NULL == tmp_ctx) {
            bRet = false;
        } else {
            if (NULL != mCtx) {
                redisFree(mCtx);
            }
            mCtx = tmp_ctx;
            bRet = auth();
        }
        return bRet;
    }

    bool auth()
    {
        bool bRet = false;
        if (0 == mPass.length()) {
            bRet = true;
        } else {
            redisReply* reply = static_cast<redisReply*>(
                redisCommand(mCtx, "AUTH %s", mPass.c_str()));
            if ((NULL == reply) || (strcasecmp(reply->str, "OK") != 0)) {
                bRet = false;
            } else {
                bRet = true;
            }
            freeReplyObject(reply);
        }
        xredis_debug("auth %s:%u %s %d", mHost.c_str(), mPort, mPass.c_str(), bRet);
        return bRet;
    }

    redisContext* mCtx;
    std::string mHost;
    std::string mPass;
    uint32_t mPort;
    uint32_t mPoolSize;
    uint32_t mIndex;
} RedisConnection;

typedef std::list<RedisConnection*> RedisConnectionList;
typedef std::list<RedisConnection*>::iterator RedisConnectionIter;

struct NodeInfo {
    std::string strinfo;
    std::string id;
    std::string ip; // The node IP
    uint16_t port; // The node port
    std::string flags; // A list of comma separated flags: myself, master, slave, fail?, fail, handshake, noaddr, noflags
    bool is_fail;
    bool is_master; // true if node is master, false if node is salve
    bool is_slave;
    std::string master_id; // The replication master
    int32_t ping_sent; // Milliseconds unix time at which the currently active ping was sent, or zero if there are no pending pings
    int32_t pong_recv; // Milliseconds unix time the last pong was received
    int32_t epoch; //
    bool connected; // The state of the link used for the node-to-node cluster
    std::vector<std::pair<uint32_t, uint32_t> > mSlots;

    bool checkSlot(uint32_t slotindex)
    {
        std::vector<std::pair<uint32_t, uint32_t> >::const_iterator citer = mSlots.begin();
        for (; citer != mSlots.end(); ++citer) {
            //xredis_debug("check %u [%u, %u]\n", slotindex, citer->first, citer->second);
            if ((slotindex >= citer->first) && (slotindex <= citer->second)) {
                return true;
            }
        }
        return false;
    }

    void parse_role(const std::string& nodeString)
    {
        const char* p = strstr(nodeString.c_str(), "master");
        if (NULL != p) {
            is_master = true;
            is_slave = false;
        }

        p = strstr(nodeString.c_str(), "slave");
        if (NULL != p) {
            is_master = false;
            is_slave = true;
        }

        p = strstr(nodeString.c_str(), "fail");
        if (NULL != p) {
            is_fail = true;
        } else {
            is_fail = false;
        }
    }

    bool parse_host(const std::string& nodeString)
    {
        std::string::size_type ColonPos = nodeString.find(':');
        if (ColonPos == std::string::npos) {
            return false;
        } else {
            const std::string port_str = nodeString.substr(ColonPos + 1);
            port = atoi(port_str.c_str());
            ip = nodeString.substr(0, ColonPos);
            return true;
        }
    }

    void parse_slot(const std::string& SlotString)
    {
        if (0 == SlotString.length()) {
            xredis_warn("SlotString is NULL ");
            return;
        }

        uint32_t StartSlot = 0;
        uint32_t EndSlot = 0;
        std::string::size_type BarPos = SlotString.find('-');
        if (BarPos == std::string::npos) {
            StartSlot = atoi(SlotString.c_str());
            EndSlot = StartSlot;
        } else {
            const std::string EndSlotStr = SlotString.substr(BarPos + 1);
            EndSlot = atoi(EndSlotStr.c_str());
            StartSlot = atoi(SlotString.substr(0, BarPos).c_str());
        }
        mSlots.push_back(std::make_pair(StartSlot, EndSlot));

        xredis_debug("%s mSlots:%u", SlotString.c_str(), mSlots.size());
    }
};

typedef std::vector<NodeInfo> NODELIST;

class ClusterInfo {
public:
    ClusterInfo() { }
    ~ClusterInfo() { }
    NODELIST nodes;
    std::string pass;
    uint32_t poolsize;
    bool clusterEnabled;
};

class xRedisClusterManager {
public:
    xRedisClusterManager();
    ~xRedisClusterManager();

private:
public:
    static bool auth(redisContext* c, const std::string& pass);
    bool connectCluster(const ClusterInfo* cluster_info);
    void keepalive();
    bool commandArgv(const VSTRING& vDataIn, RedisResult& result);
    bool command(RedisResult& result, const char* format, const char* key, va_list args);

    RedisConnection* get_cluster_info(ClusterInfo& ClusterInfo);
    bool check_cluster_info(ClusterInfo& cinfo);

    static bool Sort_asc(const NodeInfo& a, const NodeInfo& b)
    {
        return (a.id < b.id);
    }

    void bfree()
    {
        bFree = true;
    }

private:
    static uint16_t crc16(const char* buf, int32_t len);
    static bool checkReply(const redisReply* reply);
    static void freeReply(const redisReply* reply);
    static int32_t str2Vect(const char* pSrc, std::vector<std::string>& vDest,
        const char* pSep = ",");

public:
    static bool clusterEnabled(redisContext* ctx);
    static bool clusterState(redisContext* ctx);
    static bool clusterNodes(redisContext* ctx, ClusterInfo* info);
    void freeConnection(RedisConnection* pRedis);
    bool release();

    static xRedisClusterManager* connectRedis(const std::string& host, uint32_t port,
        const std::string& pass, uint32_t poolsize, ClusterInfo*& info);

private:
    bool connectRedisNode(int idx, const std::string& host, uint32_t port,
        const std::string& pass, uint32_t poolsize);
    bool checkReply(redisReply* reply);
    uint32_t keyHashSlot(const char* key, size_t keylen);

    RedisConnection* getConnection(uint32_t idx);

    uint32_t findNodeIndex(uint32_t slot);
    uint32_t getKeySlotIndex(const char* key);
    RedisConnection* findNodeConnection(const char* key);

private:
    RedisConnectionList* mRedisConnList;
    xLock* mLock;
    ClusterInfo mClusterInfo;
    bool bFree;
};

}
#endif
