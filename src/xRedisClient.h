/*
 * ----------------------------------------------------------------------------
 * Copyright (c) 2013-2014, xSky <guozhw at gmail dot com>
 * All rights reserved.
 * Distributed under GPL license.
 * ----------------------------------------------------------------------------
 */

#ifndef _XREDIS_CLIENT_H_
#define _XREDIS_CLIENT_H_

#include <stdint.h>
#include <string.h>
#include <string>
#include <vector>
#include <set>
#include <map>
#include <algorithm>
#include <sstream>

using namespace std;

#define REDIS_REPLY_STRING 1
#define REDIS_REPLY_ARRAY 2
#define REDIS_REPLY_INTEGER 3
#define REDIS_REPLY_NIL 4
#define REDIS_REPLY_STATUS 5
#define REDIS_REPLY_ERROR 6


#define MAX_ERR_STR_LEN 128

typedef std::string             KEY;
typedef std::string             VALUE;
typedef std::vector<KEY>        KEYS;
typedef KEYS                    FILEDS;
typedef std::vector<VALUE>      VALUES;
typedef std::vector<std::string>     VDATA;

typedef std::set<std::string>        SETDATA;

typedef struct _REDIS_NODE_{
    unsigned int dbindex;       //  节点编号索引，从0开始
    const char  *host;          //  REDIS节点主机IP地址
    unsigned int port;          //  redis服务端口
    const char  *passwd;        //  redis认证密码
    unsigned int poolsize;      //  此节点上的连接池大小
    unsigned int timeout;       //  连接超时时间 秒
    unsigned int role;          //  节点角色 
}RedisNode;

/* This is the reply object returned by redisCommand() */
typedef struct rReply {
    int type; /* REDIS_REPLY_* */
    long long integer; /* The integer when type is REDIS_REPLY_INTEGER */
    int len; /* Length of string */
    char *str; /* Used for both REDIS_REPLY_ERROR and REDIS_REPLY_STRING */
    size_t elements; /* number of elements, for REDIS_REPLY_ARRAY */
    struct rReply **element; /* elements vector for REDIS_REPLY_ARRAY */
} rReply;



typedef unsigned int (*HASHFUN)(const char *);

class RedisPool;
class xRedisClient;

class RedisDBIdx {
public:
    RedisDBIdx();
    RedisDBIdx(xRedisClient *xredisclient);
    ~RedisDBIdx();

    bool CreateDBIndex(const char *key,  HASHFUN fun, unsigned int type);
    bool CreateDBIndex(int64_t id, unsigned int type);
    char *GetErrInfo() {return mStrerr;}
    void SetIOMaster();

private:
    bool SetErrInfo(const char *info, int len);
    void IOtype(unsigned int type);
    friend class xRedisClient;

private:
    unsigned int mType;
    unsigned int mIndex;
    char         *mStrerr;
    xRedisClient *mClient;
    unsigned int mIOtype;
    bool         mIOFlag;
};

typedef struct _DATA_ITEM_{
    int         type;
    std::string str;
    
    _DATA_ITEM_ & operator=(const _DATA_ITEM_ &data) {
        type = data.type;
        str  = data.str;
        return *this;
    }
}DataItem;
typedef std::vector<DataItem>            ReplyData;
typedef ReplyData                        ArrayReply;
typedef std::map<std::string, double>    ZSETDATA;
typedef std::vector<RedisDBIdx>          DBIArray;

typedef struct xRedisContext_{
    void* conn;
}xRedisContext;

typedef enum _SET_TYPE_{
    TYPE_NONE = 0,
    PX = 1,
    EX  = 2
}SETPXEX;

typedef enum _SET_TYPE_NXEX_{
    TNXXX_NONE = 0,
    NX = 1,
    XX  = 2
}SETNXXX;

typedef enum _BIT_OP_{
    AND = 0,
    OR  = 1,
    XOR = 2,
    NOT = 3
}BITOP;

typedef enum _LIST_MODEL_{
    BEFORE = 0,
    AFTER  = 1
}LMODEL;


typedef enum _SORT_ORDER_{
    ASC = 0,
    DESC = 1
}SORTODER;

typedef struct _SORT_LIMIT_
{ 
	int offset; 
	int count; 
}LIMIT;

template<class T>
std::string toString(const T &t) {
    ostringstream oss;
    oss << t;
    return oss.str();
}

typedef enum _REDIS_ROLE_{
    MASTER = 0,
    SLAVE  = 1
}ROLE;

#define SETDEFAULTIOTYPE(type) if (!dbi.mIOFlag) {SetIOtype(dbi, type);}

class xRedisClient{
public:
    xRedisClient();
    ~xRedisClient();

    bool Init(unsigned int maxtype);
    void Release();
    void Keepalive();
    inline RedisPool *GetRedisPool();
    static void FreeReply(const rReply* reply);
    static int GetReply(xRedisContext* ctx, ReplyData& vData);
    bool GetxRedisContext(const RedisDBIdx& dbi, xRedisContext* ctx);
    void FreexRedisContext(xRedisContext* ctx);
    bool ConnectRedisCache(const RedisNode *redisnodelist, unsigned int nodecount, 
        unsigned int hashbase, unsigned int cachetype);

public:

    //              Connection
    /* AUTH        */  /* nonsupport */
    /* ECHO        */  bool echo(const RedisDBIdx& dbi, const std::string& str, std::string &value);
    /* PING        */  /* nonsupport */
    /* QUIT        */  void quit();
    /* SELECT      */  /* nonsupport */

    //                 Commands operating on std::string values
    /* APPEND      */  bool append(const RedisDBIdx& dbi,  const std::string& key,  const std::string& value);
    /* BITCOUNT    */  bool bitcount(const RedisDBIdx& dbi,const std::string& key, int& count, int start=0, int end=0);
    /* BITOP       */  bool bitop(const RedisDBIdx& dbi,   const BITOP operation, const std::string& destkey, const KEYS& keys, int& lenght);
    /* BITPOS      */  bool bitpos(const RedisDBIdx& dbi,  const std::string& key, int bit, int64_t& pos, int start=0, int end=0);
    /* DECR        */  bool decr(const RedisDBIdx& dbi,    const std::string& key, int64_t& result);
    /* DECRBY      */  bool decrby(const RedisDBIdx& dbi,  const std::string& key, int by, int64_t& result);
    /* GET         */  bool get(const RedisDBIdx& dbi,     const std::string& key, std::string& value);
    /* GETBIT      */  bool getbit(const RedisDBIdx& dbi,  const std::string& key, int& offset, int& bit);
    /* GETRANGE    */  bool getrange(const RedisDBIdx& dbi,const std::string& key, int start, int end, std::string& out);
    /* GETSET      */  bool getset(const RedisDBIdx& dbi,  const std::string& key, const std::string& newValue, std::string& oldValue);
    /* INCR        */  bool incr(const RedisDBIdx& dbi,    const std::string& key, int64_t& result);
    /* INCRBY      */  bool incrby(const RedisDBIdx& dbi,  const std::string& key, int by, int64_t& result);
    /* INCRBYFLOAT */  
    /* MGET        */  bool mget(const DBIArray& dbi,    const KEYS &  keys, ReplyData& vDdata);
    /* MSET        */  bool mset(const DBIArray& dbi,    const VDATA& data);
    /* MSETNX      */  
    /* PSETEX      */  bool psetex(const RedisDBIdx& dbi,  const std::string& key,  int milliseconds, const std::string& value);
    /* SET         */  bool set(const RedisDBIdx& dbi,     const std::string& key,  const std::string& value);
    /* SET         */  bool set(const RedisDBIdx& dbi,     const std::string& key, const char *value, int len, int second);
    /* SET         */  bool set(const RedisDBIdx& dbi,     const std::string& key, const std::string& value, 
        SETPXEX pxex = TYPE_NONE, int expiretime = 0, SETNXXX nxxx = TNXXX_NONE);
    /* SETBIT      */  bool setbit(const RedisDBIdx& dbi,  const std::string& key,  int offset, int64_t newbitValue, int64_t oldbitValue);
    /* SETEX       */  bool setex(const RedisDBIdx& dbi,   const std::string& key,  int seconds, const std::string& value);
    /* SETNX       */  bool setnx(const RedisDBIdx& dbi,   const std::string& key,  const std::string& value);
    /* SETRANGE    */  bool setrange(const RedisDBIdx& dbi,const std::string& key,  int offset, const std::string& value, int& length);
    /* STRLEN      */  bool strlen(const RedisDBIdx& dbi,  const std::string& key, int& length);


    /* DEL          */  bool del(const RedisDBIdx& dbi,    const std::string& key);
                        bool del(const DBIArray& dbi,      const KEYS &  vkey, int64_t& count);
    /* DUMP         */
    /* EXISTS       */  bool exists(const RedisDBIdx& dbi, const std::string& key);
    /* EXPIRE       */  bool expire(const RedisDBIdx& dbi, const std::string& key, unsigned int second);
    /* EXPIREAT     */  bool expireat(const RedisDBIdx& dbi, const std::string& key, unsigned int timestamp);
    /* KEYS         */  
    /* MIGRATE      */  
    /* MOVE         */  
    /* OBJECT       */  
    /* PERSIST      */  bool persist(const RedisDBIdx& dbi, const std::string& key);
    /* PEXPIRE      */  bool pexpire(const RedisDBIdx& dbi, const std::string& key, unsigned int milliseconds);
    /* PEXPIREAT    */  bool pexpireat(const RedisDBIdx& dbi, const std::string& key, unsigned int millisecondstimestamp);
    /* PTTL         */  bool pttl(const RedisDBIdx& dbi, const std::string& key,  int64_t &milliseconds);
    /* RANDOMKEY    */  bool randomkey(const RedisDBIdx& dbi,  KEY& key);
    /* RENAME       */  
    /* RENAMENX     */  
    /* RESTORE      */       
    /* SCAN         */  bool scan(const RedisDBIdx& dbi, int64_t &cursor, 
        const char *pattern, uint32_t count, ArrayReply& array, xRedisContext& ctx);

    
    /* SORT         */  bool sort(const RedisDBIdx& dbi, ArrayReply& array, const std::string& key, const char* by = NULL,
                                    LIMIT *limit = NULL, bool alpha = false, const FILEDS* get = NULL, 
                                    const SORTODER order = ASC, const char* destination = NULL);

    /* TTL          */  bool ttl(const RedisDBIdx& dbi, const std::string& key, int64_t& seconds);
    /* TYPE         */  bool type(const RedisDBIdx& dbi, const std::string& key, std::string& value);


    /* HDEL         */  bool hdel(const RedisDBIdx& dbi,    const std::string& key, const std::string& field, int64_t& num);
                        bool hdel(const RedisDBIdx& dbi,    const std::string& key, const KEYS& vfiled, int64_t& num);
    /* HEXISTS      */  bool hexist(const RedisDBIdx& dbi,  const std::string& key, const std::string& field);
    /* HGET         */  bool hget(const RedisDBIdx& dbi,    const std::string& key, const std::string& field, std::string& value);
    /* HGETALL      */  bool hgetall(const RedisDBIdx& dbi, const std::string& key, ArrayReply& array);
    /* HINCRBY      */  bool hincrby(const RedisDBIdx& dbi, const std::string& key, const std::string& field, int64_t increment ,int64_t& value);
    /* HINCRBYFLOAT */  bool hincrbyfloat(const RedisDBIdx& dbi,  const std::string& key, const std::string& field, const float increment, float& value);
    /* HKEYS        */  bool hkeys(const RedisDBIdx& dbi,   const std::string& key, KEYS& keys);
    /* HLEN         */  bool hlen(const RedisDBIdx& dbi,    const std::string& key, int64_t& count);
    /* HMGET        */  bool hmget(const RedisDBIdx& dbi,   const std::string& key, const KEYS& field, ArrayReply& array);
    /* HMSET        */  bool hmset(const RedisDBIdx& dbi,   const std::string& key, const VDATA& vData);
    /* HSCAN        */ bool  hscan(const RedisDBIdx& dbi, const std::string& key, int64_t &cursor,
                                const char *pattern, uint32_t count, ArrayReply& array, xRedisContext& ctx);
    /* HSET         */  bool hset(const RedisDBIdx& dbi,    const std::string& key, const std::string& field, const std::string& value, int64_t& retval);
    /* HSETNX       */  bool hsetnx(const RedisDBIdx& dbi,  const std::string& key, const std::string& field, const std::string& value);
    /* HVALS        */  bool hvals(const RedisDBIdx& dbi,   const std::string& key, VALUES& values);

    /* BLPOP        */  bool blPop(const RedisDBIdx& dbi,    const std::string& key, VALUES& vValues, int64_t timeout);
    /* BRPOP        */  bool brPop(const RedisDBIdx& dbi,    const std::string& key, VALUES& vValues, int64_t timeout);
    /* BRPOPLPUSH   */  bool brPoplpush(const RedisDBIdx& dbi, const std::string& key, std::string& targetkey, VALUE& value, int64_t timeout);
    /* LINDEX       */  bool lindex(const RedisDBIdx& dbi,    const std::string& key, int64_t index, VALUE& value);
    /* LINSERT      */  bool linsert(const RedisDBIdx& dbi,  const std::string& key, LMODEL mod, const std::string& pivot, const std::string& value, int64_t& retval);
    /* LLEN         */  bool llen(const RedisDBIdx& dbi,     const std::string& key, int64_t& len);
    /* LPOP         */  bool lpop(const RedisDBIdx& dbi,     const std::string& key, std::string& value);
    /* LPUSH        */  bool lpush(const RedisDBIdx& dbi,    const std::string& key, const VALUES& vValue, int64_t& length);
    /* LPUSHX       */  bool lpushx(const RedisDBIdx& dbi,   const std::string& key, const std::string& value, int64_t& length);
    /* LRANGE       */  bool lrange(const RedisDBIdx& dbi,   const std::string& key, int64_t start, int64_t end, ArrayReply& array);
    /* LREM         */  bool lrem(const RedisDBIdx& dbi,     const std::string& key,  int count, const std::string& value, int64_t num);
    /* LSET         */  bool lset(const RedisDBIdx& dbi,     const std::string& key,  int index, const std::string& value);
    /* LTRIM        */  bool ltrim(const RedisDBIdx& dbi,    const std::string& key,  int start, int end);
    /* RPOP         */  bool rpop(const RedisDBIdx& dbi,     const std::string& key, std::string& value);
    /* RPOPLPUSH    */  bool rpoplpush(const RedisDBIdx& dbi,const std::string& key_src, const std::string& key_dest, std::string& value);
    /* RPUSH        */  bool rpush(const RedisDBIdx& dbi,    const std::string& key, const VALUES& vValue, int64_t& length);
    /* RPUSHX       */  bool rpushx(const RedisDBIdx& dbi,   const std::string& key, const std::string& value, int64_t& length);

    /* SADD         */  bool sadd(const RedisDBIdx& dbi,        const KEY& key, const VALUES& vValue, int64_t& count);
    /* SCARD        */  bool scard(const RedisDBIdx& dbi, const KEY& key, int64_t& count);
    /* SDIFF        */  bool sdiff(const DBIArray& dbi,       const KEYS& vKkey, VALUES& vValue);
    /* SDIFFSTORE   */  bool sdiffstore(const RedisDBIdx& dbi,  const KEY& destinationkey, const DBIArray& vdbi, const KEYS& vkey, int64_t& count);
    /* SINTER       */  bool sinter(const DBIArray& dbi,      const KEYS& vkey, VALUES& vValue);
    /* SINTERSTORE  */  bool sinterstore(const RedisDBIdx& dbi, const KEY& destinationkey, const DBIArray& vdbi, const KEYS& vkey, int64_t& count);
    /* SISMEMBER    */  bool sismember(const RedisDBIdx& dbi,   const KEY& key,   const VALUE& member);
    /* SMEMBERS     */  bool smembers(const RedisDBIdx& dbi,     const KEY& key,  VALUES& vValue);
    /* SMOVE        */  bool smove(const RedisDBIdx& dbi,       const KEY& srckey, const KEY& deskey,  const VALUE& member);
    /* SPOP         */  bool spop(const RedisDBIdx& dbi,        const KEY& key, VALUE& member);
    /* SRANDMEMBER  */  bool srandmember(const RedisDBIdx& dbi, const KEY& key, VALUES& vmember, int num=0);
    /* SREM         */  bool srem(const RedisDBIdx& dbi,        const KEY& key, const VALUES& vmembers, int64_t& count);
    /* SSCAN        */  bool sscan(const RedisDBIdx& dbi, const std::string& key, int64_t &cursor,
        const char *pattern, uint32_t count, ArrayReply& array, xRedisContext& ctx);
    /* SUNION       */  bool sunion(const DBIArray& dbi,      const KEYS& vkey, VALUES& vValue);
    /* SUNIONSTORE  */  bool sunionstore(const RedisDBIdx& dbi, const KEY& deskey, const DBIArray& vdbi, const KEYS& vkey, int64_t& count);

    /* ZADD             */  bool zadd(const RedisDBIdx& dbi,    const KEY& deskey,   const VALUES& vValues, int64_t& count);
    /* ZCARD            */  bool zscrad(const RedisDBIdx& dbi,  const std::string& key, int64_t& num);
    /* ZCOUNT           */
    /* ZINCRBY          */  bool zincrby(const RedisDBIdx& dbi, const std::string& key, const double &increment, const std::string& member, std::string& value );
    /* ZINTERSTORE      */  
    /* ZRANGE           */  bool zrange(const RedisDBIdx& dbi,  const std::string& key, int start, int end, VALUES& vValues, bool withscore=false);
    /* ZRANGEBYSCORE    */  
    /* ZRANK            */  bool zrank(const RedisDBIdx& dbi,   const std::string& key, const std::string& member, int64_t &rank);
    /* ZREM             */  bool zrem(const RedisDBIdx& dbi,    const KEY& key, const VALUES& vmembers, int64_t &num);
    /* ZREMRANGEBYRANK  */  bool zremrangebyrank(const RedisDBIdx& dbi,  const std::string& key, int start, int stop, int64_t& num);
    /* ZREMRANGEBYSCORE */  
    /* ZREVRANGE        */  bool zrevrange(const RedisDBIdx& dbi,  const std::string& key, int start, int end, VALUES& vValues, bool withscore=false);
    /* ZREVRANGEBYLEX   */  bool zrevrangebylex(const RedisDBIdx& dbi, const string& key, string& start, string& end, VALUES& vValues, int offset = 0, int count = 0);
    /* ZREVRANGEBYSCORE */  
    /* ZREVRANK         */  bool zrevrank(const RedisDBIdx& dbi,  const std::string& key, const std::string &member, int64_t& rank);
    /* ZSCAN            */  bool zscan(const RedisDBIdx& dbi, const std::string& key, int64_t &cursor, const char *pattern,
        uint32_t count, ArrayReply& array, xRedisContext& ctx);
    /* ZSCORE           */  bool zscore(const RedisDBIdx& dbi,  const std::string& key, const std::string &member, std::string& score);
    /* ZUNIONSTORE      */  

    /*  注意： 使用下面的 pub/sub 命令时，一定要保证数据都在同一个REDIS实例里，xredis目前的pub/sub命令实现不支持多节点数据分布的场景。  */
    /* PSUBSCRIBE   */     bool psubscribe(const RedisDBIdx& dbi, const KEYS& patterns, xRedisContext& ctx);
    /* PUBLISH      */     bool publish(const RedisDBIdx& dbi, const KEY& channel, const std::string& message, int64_t& count);
    /* PUBSUB       */     bool pubsub_channels(const RedisDBIdx& dbi, const std::string &pattern, ArrayReply &reply);
                           bool pubsub_numsub(const RedisDBIdx& dbi, const KEYS &keys, ArrayReply &reply);
                           bool pubsub_numpat(const RedisDBIdx& dbi, int64_t& count);
    /* PUNSUBSCRIBE */     bool punsubscribe(const RedisDBIdx& dbi, const KEYS& patterns, xRedisContext& ctx);
    /* SUBSCRIBE    */     bool subscribe(const RedisDBIdx& dbi, const KEYS& channels, xRedisContext& ctx);
    /* UNSUBSCRIBE  */     bool unsubscribe(const RedisDBIdx& dbi, const KEYS& channels, xRedisContext& ctx);


    /* DISCARD  */
    /* EXEC     */
    /* MULTI    */
    /* UNWATCH  */
    /* WATCH    */


private:
    void addparam(VDATA& vDes, const VDATA& vSrc) {
        for (VDATA::const_iterator iter=vSrc.begin(); iter!=vSrc.end();++iter) {
            vDes.push_back(*iter);
        }
    }
    void SetErrInfo(const RedisDBIdx& dbi, void *p);
    void SetErrString(const RedisDBIdx& dbi, const char *str, int len);
    void SetErrMessage(const RedisDBIdx& dbi, const char* fmt, ...);
    void SetIOtype(const RedisDBIdx& dbi, unsigned int iotype, bool ioflag = false);
    bool ScanFun(const char* cmd, const RedisDBIdx& dbi, const std::string *key, int64_t &cursor,
        const char* pattern, uint32_t count, ArrayReply& array, xRedisContext& ctx);

public:

    bool command_bool(const RedisDBIdx& dbi,                       const char* cmd, ...);
    bool command_status(const RedisDBIdx& dbi,                     const char* cmd, ...);
    bool command_integer(const RedisDBIdx& dbi, int64_t &intval,   const char* cmd, ...);
    bool command_string(const RedisDBIdx& dbi,  std::string &data, const char* cmd, ...);
    bool command_list(const RedisDBIdx& dbi,    VALUES &vValue,    const char* cmd, ...);
    bool command_array(const RedisDBIdx& dbi,   ArrayReply& array, const char* cmd, ...);
    rReply *command(const RedisDBIdx& dbi, const char* cmd);
private:
    bool commandargv_bool(const RedisDBIdx& dbi,   const VDATA& vData);
    bool commandargv_status(const RedisDBIdx& dbi, const VDATA& vData);
    bool commandargv_array(const RedisDBIdx& dbi,  const VDATA& vDataIn, ArrayReply& array);
    bool commandargv_array(const RedisDBIdx& dbi,  const VDATA& vDataIn, VALUES& array);
    bool commandargv_integer(const RedisDBIdx& dbi,const VDATA& vDataIn, int64_t& retval);
    bool commandargv_array_ex(const RedisDBIdx& dbi, const VDATA& vDataIn, xRedisContext& ctx);
private:
    RedisPool *mRedisPool;
};



#endif





