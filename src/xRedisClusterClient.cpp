#include "xRedisClusterClient.h"


static const uint16_t crc16tab[256]= {
    0x0000,0x1021,0x2042,0x3063,0x4084,0x50a5,0x60c6,0x70e7,
    0x8108,0x9129,0xa14a,0xb16b,0xc18c,0xd1ad,0xe1ce,0xf1ef,
    0x1231,0x0210,0x3273,0x2252,0x52b5,0x4294,0x72f7,0x62d6,
    0x9339,0x8318,0xb37b,0xa35a,0xd3bd,0xc39c,0xf3ff,0xe3de,
    0x2462,0x3443,0x0420,0x1401,0x64e6,0x74c7,0x44a4,0x5485,
    0xa56a,0xb54b,0x8528,0x9509,0xe5ee,0xf5cf,0xc5ac,0xd58d,
    0x3653,0x2672,0x1611,0x0630,0x76d7,0x66f6,0x5695,0x46b4,
    0xb75b,0xa77a,0x9719,0x8738,0xf7df,0xe7fe,0xd79d,0xc7bc,
    0x48c4,0x58e5,0x6886,0x78a7,0x0840,0x1861,0x2802,0x3823,
    0xc9cc,0xd9ed,0xe98e,0xf9af,0x8948,0x9969,0xa90a,0xb92b,
    0x5af5,0x4ad4,0x7ab7,0x6a96,0x1a71,0x0a50,0x3a33,0x2a12,
    0xdbfd,0xcbdc,0xfbbf,0xeb9e,0x9b79,0x8b58,0xbb3b,0xab1a,
    0x6ca6,0x7c87,0x4ce4,0x5cc5,0x2c22,0x3c03,0x0c60,0x1c41,
    0xedae,0xfd8f,0xcdec,0xddcd,0xad2a,0xbd0b,0x8d68,0x9d49,
    0x7e97,0x6eb6,0x5ed5,0x4ef4,0x3e13,0x2e32,0x1e51,0x0e70,
    0xff9f,0xefbe,0xdfdd,0xcffc,0xbf1b,0xaf3a,0x9f59,0x8f78,
    0x9188,0x81a9,0xb1ca,0xa1eb,0xd10c,0xc12d,0xf14e,0xe16f,
    0x1080,0x00a1,0x30c2,0x20e3,0x5004,0x4025,0x7046,0x6067,
    0x83b9,0x9398,0xa3fb,0xb3da,0xc33d,0xd31c,0xe37f,0xf35e,
    0x02b1,0x1290,0x22f3,0x32d2,0x4235,0x5214,0x6277,0x7256,
    0xb5ea,0xa5cb,0x95a8,0x8589,0xf56e,0xe54f,0xd52c,0xc50d,
    0x34e2,0x24c3,0x14a0,0x0481,0x7466,0x6447,0x5424,0x4405,
    0xa7db,0xb7fa,0x8799,0x97b8,0xe75f,0xf77e,0xc71d,0xd73c,
    0x26d3,0x36f2,0x0691,0x16b0,0x6657,0x7676,0x4615,0x5634,
    0xd94c,0xc96d,0xf90e,0xe92f,0x99c8,0x89e9,0xb98a,0xa9ab,
    0x5844,0x4865,0x7806,0x6827,0x18c0,0x08e1,0x3882,0x28a3,
    0xcb7d,0xdb5c,0xeb3f,0xfb1e,0x8bf9,0x9bd8,0xabbb,0xbb9a,
    0x4a75,0x5a54,0x6a37,0x7a16,0x0af1,0x1ad0,0x2ab3,0x3a92,
    0xfd2e,0xed0f,0xdd6c,0xcd4d,0xbdaa,0xad8b,0x9de8,0x8dc9,
    0x7c26,0x6c07,0x5c64,0x4c45,0x3ca2,0x2c83,0x1ce0,0x0cc1,
    0xef1f,0xff3e,0xcf5d,0xdf7c,0xaf9b,0xbfba,0x8fd9,0x9ff8,
    0x6e17,0x7e36,0x4e55,0x5e74,0x2e93,0x3eb2,0x0ed1,0x1ef0
};

 uint16_t xRedisClusterClient::crc16(const char *buf, int len) {
    int counter;
    uint16_t crc = 0;
    for (counter = 0; counter < len; counter++)
            crc = (crc<<8) ^ crc16tab[((crc>>8) ^ *buf++)&0x00FF];
    return crc;
}

 bool xRedisClusterClient::CheckReply(const redisReply *reply){
    if (NULL == reply) {
        return false;
    }

    switch (reply->type){
    case REDIS_REPLY_STRING:{
        return true;
    }
    case REDIS_REPLY_ARRAY:{
        return true;
    }
    case REDIS_REPLY_INTEGER:{
        return true;
    }
    case REDIS_REPLY_NIL:{
        return false;
    }
    case REDIS_REPLY_STATUS:{
        return true;
    }
    case REDIS_REPLY_ERROR:{
        return false;
    }
    default:{
        return false;
    }
    }

    return false;
}

void xRedisClusterClient::FreeReply(const redisReply *reply){
    if (NULL != reply) {
        freeReplyObject((void*)reply);
    }
}

int xRedisClusterClient::str2Vect(const char* pSrc, vector<string> &vDest, const char *pSep) {
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

xRedisClusterClient::xRedisClusterClient()
{
    mRedisConnList = NULL;
    mLock = NULL;
    mClusterEnabled = false;
    mPoolSize = 4;
}
 
xRedisClusterClient::~xRedisClusterClient()
{
    Release();
}

bool xRedisClusterClient::GetClusterNodes(redisContext *redis_ctx)
{
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
    }

    freeReplyObject(redis_reply);
    return true;
}

void xRedisClusterClient::Keepalive()
{
    //RedisConnIter iter = mRedisConnList.begin();
    //for (; iter != mRedisConnList.end(); iter++) {
    //    redisReply *reply = (redisReply *)redisCommand((*iter)->c,"PING");
    //    printf("PING: %s\n", reply->str);
    //    freeReplyObject(reply);
    //}

    size_t node_count = vNodes.size();
    for (size_t i = 0; i < node_count; ++i) {
        XLOCK(mLock[i]);
        RedisConnectionList *pList = &mRedisConnList[i];
        RedisConnectionIter iter = pList->begin();
        for (; iter != pList->end(); iter++) {
            RedisConnection *pConn = *iter;
            redisReply *reply = static_cast<redisReply *>(redisCommand(pConn->mCtx, "PING"));
            bool bRet = (NULL != reply);
            if (bRet) {
                freeReplyObject(reply);
            }
        }
    }

}

void xRedisClusterClient::Release()
{
    //XLOCK(mLock);
    size_t node_count = vNodes.size();
    for (size_t i = 0; i < node_count; ++i) {
        RedisConnectionList *pList = &mRedisConnList[i];
        RedisConnectionIter iter = pList->begin();
        for (; iter != pList->end(); iter++) {
            redisFree((*iter)->mCtx);
            delete *iter;
        }
    }
    vNodes.clear();
    delete [] mRedisConnList;
    mRedisConnList = NULL;
    delete [] mLock;
    mLock = NULL;
}

#define REDIS_REPLY_STRING 1
#define REDIS_REPLY_ARRAY 2
#define REDIS_REPLY_INTEGER 3
#define REDIS_REPLY_NIL 4
#define REDIS_REPLY_STATUS 5
#define REDIS_REPLY_ERROR 6

bool xRedisClusterClient::CheckReply(redisReply *reply)
{
    if(NULL==reply)  {
       printf("error, reply is NULL \r\n");
       return false;
    }
    
    printf("DEBBUG %d:%s \r\n", reply->type, reply->str);

    return true;
}

bool xRedisClusterClient::ClusterEnabled(redisContext *ctx)
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

bool xRedisClusterClient::Clusterinfo(redisContext *ctx)
{
    redisReply *redis_reply = (redisReply*)redisCommand(ctx, "CLUSTER info");
    if( (NULL==redis_reply) ||(NULL==redis_reply->str)) {
        if(redis_reply) {
            freeReplyObject(redis_reply);
        }
        return false;
    }
    //printf("Clusterinfo:\r\n%s\r\n", redis_reply->str);
    char *p = strstr(redis_reply->str, ":");
    bool bRet = (0 == strncmp(p+1, "ok", 2));
    freeReplyObject(redis_reply);
    return bRet;
}

bool xRedisClusterClient::ReConnectRedis(RedisConnection *pConn)
{   
    Release();

    return ConnectRedis(pConn->mHost, pConn->mPort, pConn->mPoolSize);
}

bool xRedisClusterClient::ConnectRedis(const char *host, uint32_t port, uint32_t poolsize)
{
    if (NULL == host) {
        printf("error argv \r\n");
        return false;
    }

    struct timeval timeoutVal;
    timeoutVal.tv_sec = 5;
    timeoutVal.tv_usec = 0;
    mPoolSize = poolsize;

    redisContext *redis_ctx = redisConnectWithTimeout(host, port, timeoutVal);
    if (redis_ctx == NULL || redis_ctx->err) {
        if (redis_ctx) {
            printf("Connection error: %s \r\n", redis_ctx->errstr);
            redisFree(redis_ctx);
            return false;
        } else {
            printf("Connection error: can't allocate redis context, %s  \n", redis_ctx->errstr);
        }
    } else {
        printf("Connect to Redis: %s:%d  success \n", host, port);
    }

    mClusterEnabled = ClusterEnabled(redis_ctx);
    printf("ClusterEnabled %d \r\n", mClusterEnabled);

    if (!mClusterEnabled) {
        mRedisConnList = new RedisConnectionList[1];
        ConnectRedisNode(0, host, port, mPoolSize);
        return true;
    }

    if(!Clusterinfo(redis_ctx)) {
        printf("Clusterinfo error \r\n");
        return false;
    }
    
    vector<string> vlines;
    redisReply *redis_reply = (redisReply*)redisCommand(redis_ctx, "CLUSTER NODES");
    if( (NULL==redis_reply) ||(NULL==redis_reply->str)) {
        if(redis_reply) {
            freeReplyObject(redis_reply);
        }
        redisFree(redis_ctx);
        return false;
    }
    
    str2Vect(redis_reply->str, vlines, "\n");
    printf("vlines:%lu\r\n", vlines.size());
    
    for (size_t i= 0; i < vlines.size(); ++i) {
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
    }

    freeReplyObject(redis_reply);
    redisFree(redis_ctx);

//    XLOCK(mLock);
    int cnt = vNodes.size();
    mRedisConnList = new RedisConnectionList[cnt];
    mLock = new xLock[cnt];
    for (int i = 0; i < cnt; ++i) {
        ConnectRedisNode(i, vNodes[i].ip.c_str(), vNodes[i].port, mPoolSize);
    }

    return true;
}

bool xRedisClusterClient::ConnectRedisNode(int idx, const char *host, uint32_t port, uint32_t poolsize)
{
    if(NULL==host) {
        printf("error argv \r\n");
        return false;
    }
    
    //同时打开 CONNECTION_NUM 个连接
    try
    {
        poolsize = poolsize>MAX_REDIS_POOLSIZE?MAX_REDIS_POOLSIZE:poolsize;

        for (uint32_t i = 0; i < poolsize; ++i)
        {
            struct timeval timeoutVal;
            timeoutVal.tv_sec = MAX_TIME_OUT;
            timeoutVal.tv_usec = 0;
            
            RedisConnection *pRedisconn = new RedisConnection;
            if(NULL==pRedisconn) {
                printf("error pRedisconn is null, %s %d \r\n", host, port);
                continue;
            }
            pRedisconn->mHost  = host;
            pRedisconn->mPort = port;
            pRedisconn->mPoolSize = poolsize;
            pRedisconn->mIndex = idx;
            pRedisconn->mCtx = redisConnectWithTimeout(host, port, timeoutVal);
            if (pRedisconn->mCtx == NULL || pRedisconn->mCtx->err) {
                if (pRedisconn->mCtx) {
                    printf("Connection error: %s \r\n", pRedisconn->mCtx->errstr);
                    redisFree(pRedisconn->mCtx);
                } else {
                    printf("Connection error: can't allocate redis context, %s  \n", pRedisconn->mCtx->errstr);
                }
                delete pRedisconn;
            } else {
                printf("Connect to Redis[%u]: %s:%d poolsize:%d success \n",
                      idx, host, port, poolsize);
                mRedisConnList[idx].push_back(pRedisconn);
            }
        }
    } catch( ...) {
        printf("connect error  poolsize=%d \n", poolsize);
        return false;
    }
    return true;
}

RedisConnection *xRedisClusterClient::GetConnection(uint32_t idx)
{
    RedisConnection *pRedisConn = NULL;

    while (1) { {
            XLOCK(mLock[idx]);
            if (mRedisConnList[idx].size() > 0) {
                pRedisConn = mRedisConnList[idx].front();
                mRedisConnList[idx].pop_front();
                break;
            } else {
                printf("RedisPool::GetConnection()  error pthread_id=%lu \n", pthread_self());
            }
        }
        usleep(1000);
    }
    printf("idx:%u %s:%u \n", idx, pRedisConn->mHost, pRedisConn->mPort);
    return pRedisConn;
}

void xRedisClusterClient::FreeConnection(RedisConnection *pRedisConn)
{
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
uint32_t xRedisClusterClient::KeyHashSlot(const char *key, size_t keylen) {
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

uint32_t xRedisClusterClient::FindNodeIndex(uint32_t slot)
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

uint32_t xRedisClusterClient::GetKeySlotIndex(const char* key)
{
    if (NULL != key) {
        return KeyHashSlot(key, strlen(key));
    }
    return 0;
}

RedisConnection *xRedisClusterClient::FindNodeConnection(const char *key)
{
    if (!mClusterEnabled) {
        return GetConnection(0);
    }
    printf("key:%s \r\n", key);
    uint32_t slot_id = GetKeySlotIndex(key);
    uint32_t index = FindNodeIndex(slot_id);
    return GetConnection(index);
}

bool xRedisClusterClient::RedisCommand(RedisResult &result, const char *format, ...)
{
    char *key = NULL;
    bool bRet = false;
    RedisConnection *pRedisConn = NULL;

    va_list args;
    va_start(args, format);
    key = va_arg(args, char*);
    printf("key:%p \n", key);
    if(0==strlen(key)) {
        printf("key is NULL key:%p \n", key);
        return false;
    }
    pRedisConn = FindNodeConnection(key);

    va_start(args, format);
    redisReply *reply = static_cast<redisReply *>(redisvCommand(pRedisConn->mCtx, format, args));
    va_end(args);
    
    if (CheckReply(reply)) {
        result.Init(reply);

        printf("%d %lld %d %s %lu \r\n", 
            reply->type, reply->integer, reply->len, reply->str, reply->elements);

        bRet = true;
    } else {
        bRet = false;
    }

    FreeConnection(pRedisConn);
    return bRet;
}

bool xRedisClusterClient::RedisCommandArgv(const VSTRING& vDataIn, RedisResult &result){
    bool bRet = false;
    const std::string &key = vDataIn[1];
    RedisConnection *pRedisConn = FindNodeConnection(key.c_str());
    if (NULL == pRedisConn) {
        return false;
    }

    vector<const char*> argv(vDataIn.size());
    vector<size_t> argvlen(vDataIn.size());
    unsigned int j = 0;
    for (VSTRING::const_iterator i = vDataIn.begin(); i != vDataIn.end(); ++i, ++j) {
        argv[j] = i->c_str(), argvlen[j] = i->size();
    }

    redisReply *reply = static_cast<redisReply *>(redisCommandArgv(pRedisConn->mCtx, argv.size(), &(argv[0]), &(argvlen[0])));
    if (xRedisClusterClient::CheckReply(reply)) {
        result.Init(reply);
        printf("%d %lld %d %s %lu \r\n",
            reply->type, reply->integer, reply->len, reply->str, reply->elements);
        bRet = true;
    } else {
        //SetErrInfo(dbi, reply);
    }

    FreeReply(reply);
    FreeConnection(pRedisConn);
    return bRet;
}



