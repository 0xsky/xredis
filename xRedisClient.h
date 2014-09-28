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
typedef std::vector<string>     VDATA;

typedef std::set<string>        SETDATA;

typedef struct _REDIS_NODE_{
    unsigned int dbindex;
    const char  *host;
    unsigned int port;
    const char  *passwd;
    unsigned int poolsize;
    unsigned int timeout;
}RedisNode;

typedef unsigned int (*HASHFUN)(const char *);

class RedisPool;
class xRedisClient;

class RedisDBIdx {
public:
    RedisDBIdx();
    RedisDBIdx(xRedisClient *xredisclient);
    ~RedisDBIdx();

    bool CreateDBIndex(const char *key,  HASHFUN fun, const unsigned int type);
    bool CreateDBIndex(const int64_t id, const unsigned int type);
    char *GetErrInfo() {return mStrerr;}

private:
    bool SetErrInfo(const char *info, int len);
    friend class xRedisClient;

private:
    unsigned int mType;
    unsigned int mIndex;
    char        *mStrerr;
    xRedisClient *mClient;
};

typedef struct _DATA_ITEM_{
    int    type;
    string str;
    
    _DATA_ITEM_ & operator=(const _DATA_ITEM_ &data) {
        type = data.type;
        str  = data.str;
        return *this;
    }
}DataItem;
typedef std::vector<DataItem>       ReplyData;
typedef ReplyData                   ArrayReply;
typedef std::map<string, double>    ZSETDATA;
typedef std::vector<RedisDBIdx>    DBIArray;


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
string toString(const T &t) {
    ostringstream oss;
    oss << t;
    return oss.str();
}

class xRedisClient{
public:
    xRedisClient();
    virtual ~xRedisClient();

    bool Init(unsigned int maxtype);
    void release();
    void KeepAlive();
    //RedisDBIdx GetDBIndex(const char *key,  HASHFUN fun, const unsigned int type);
    //RedisDBIdx GetDBIndex(const int64_t uid, const unsigned int type);
    inline RedisPool *GetRedisPool();
    bool ConnectRedisCache( const RedisNode *redisnodelist, unsigned int hashbase, unsigned int cachetype);

public:

    //              connection
    /* AUTH        */  bool auth(const RedisDBIdx& dbi, const string& pass);
    /* ECHO        */  
    /* PING        */  
    /* QUIT        */  
    /* SELECT      */  

    //                 Commands operating on string values
    /* APPEND      */  bool append(const RedisDBIdx& dbi,  const string& key,  const string& value);
    /* BITCOUNT    */  bool bitcount(const RedisDBIdx& dbi,const string& key, int& count, const int start=0, const int end=0);
    /* BITOP       */  bool bitop(const RedisDBIdx& dbi,   const BITOP operation, const string& destkey, const KEYS& keys, int& lenght);
    /* BITPOS      */  bool bitpos(const RedisDBIdx& dbi,  const string& key, const int bit, int64_t& pos, const int start=0, const int end=0);
    /* DECR        */  bool decr(const RedisDBIdx& dbi,    const string& key, int64_t& result);
    /* DECRBY      */  bool decrby(const RedisDBIdx& dbi,  const string& key, const int by, int64_t& result);
    /* GET         */  bool get(const RedisDBIdx& dbi,     const string& key, string& value);
    /* GETBIT      */  bool getbit(const RedisDBIdx& dbi,  const string& key, const int& offset, int& bit);
    /* GETRANGE    */  bool getrange(const RedisDBIdx& dbi,const string& key, const int start, const int end, string& out);
    /* GETSET      */  bool getset(const RedisDBIdx& dbi,  const string& key, const string& newValue, string& oldValue);
    /* INCR        */  bool incr(const RedisDBIdx& dbi,    const string& key, int64_t& result);
    /* INCRBY      */  bool incrby(const RedisDBIdx& dbi,  const string& key, const int by, int64_t& result);
    /* INCRBYFLOAT */  
    /* MGET        */  bool mget(const DBIArray& dbi,    const KEYS &  keys, ReplyData& vDdata);
    /* MSET        */  bool mset(const DBIArray& dbi,    const VDATA& data);
    /* MSETNX      */  
    /* PSETEX      */  bool psetex(const RedisDBIdx& dbi,  const string& key,  const int milliseconds, const string& value);
    /* SET         */  bool set(const RedisDBIdx& dbi,     const string& key,  const string& value);
    /* SET         */  bool set(const RedisDBIdx& dbi,     const string& key, const char *value, int len, const int second=0);
    /* SETBIT      */  bool setbit(const RedisDBIdx& dbi,  const string& key,  const int offset, const int64_t newbitValue, int64_t oldbitValue);
    /* SETEX       */  bool setex(const RedisDBIdx& dbi,   const string& key,  const int seconds, const string& value);
    /* SETNX       */  bool setnx(const RedisDBIdx& dbi,   const string& key,  const string& value);
    /* SETRANGE    */  bool setrange(const RedisDBIdx& dbi,const string& key,  const int offset, const string& value, int& length);
    /* STRLEN      */  bool strlen(const RedisDBIdx& dbi,  const string& key, int& length);


    /* DEL          */  bool del(const RedisDBIdx& dbi,    const string& key);
                        bool del(const DBIArray& dbi,      const KEYS &  vkey, int64_t& count);
    /* DUMP         */
    /* EXISTS       */  bool exists(const RedisDBIdx& dbi, const string& key);
    /* EXPIRE       */  bool expire(const RedisDBIdx& dbi, const string& key, const unsigned int second);
    /* EXPIREAT     */  bool expireat(const RedisDBIdx& dbi, const string& key, const unsigned int timestamp);
    /* KEYS         */  
    /* MIGRATE      */  
    /* MOVE         */  
    /* OBJECT       */  
    /* PERSIST      */  bool persist(const RedisDBIdx& dbi, const string& key);
    /* PEXPIRE      */  bool pexpire(const RedisDBIdx& dbi, const string& key, const unsigned int milliseconds);
    /* PEXPIREAT    */  bool pexpireat(const RedisDBIdx& dbi, const string& key, const unsigned int millisecondstimestamp);
    /* PTTL         */  bool pttl(const RedisDBIdx& dbi, const string& key,  int64_t &milliseconds);
    /* RANDOMKEY    */  bool randomkey(const RedisDBIdx& dbi,  KEY& key);
    /* RENAME       */  
    /* RENAMENX     */  
    /* RESTORE      */  
    /* SCAN         */  

    
    /* SORT         */  bool sort(const RedisDBIdx& dbi, ArrayReply& array, const string& key, const char* by = NULL,
        LIMIT *limit = NULL, bool alpha = false, const FILEDS* get = NULL, 
        const SORTODER order = ASC, const char* destination = NULL);

    /* TTL          */  bool ttl(const RedisDBIdx& dbi, const string& key, int64_t& seconds);
    /* TYPE         */  


    /* HDEL         */  bool hdel(const RedisDBIdx& dbi,    const string& key, const string& filed, int64_t& num);
                        bool hdel(const RedisDBIdx& dbi,    const string& key, const KEYS& vfiled, int64_t& num);
    /* HEXISTS      */  bool hexist(const RedisDBIdx& dbi,  const string& key, const string& filed);
    /* HGET         */  bool hget(const RedisDBIdx& dbi,    const string& key, const string& filed, string& value);
    /* HGETALL      */  bool hgetall(const RedisDBIdx& dbi, const string& key, ArrayReply& array);
    /* HINCRBY      */  bool hincrby(const RedisDBIdx& dbi, const string& key, const string& filed, const int64_t increment ,int64_t& value);
    /* HINCRBYFLOAT */  bool hincrbyfloat(const RedisDBIdx& dbi,  const string& key, const string& filed, const float increment, float& value);
    /* HKEYS        */  bool hkeys(const RedisDBIdx& dbi,   const string& key, KEYS& keys);
    /* HLEN         */  bool hlen(const RedisDBIdx& dbi,    const string& key, int64_t& count);
    /* HMGET        */  bool hmget(const RedisDBIdx& dbi,   const string& key, const KEYS& filed, ArrayReply& array);
    /* HMSET        */  bool hmset(const RedisDBIdx& dbi,   const string& key, const VDATA& vData);
    /* HSCAN        */                                      
    /* HSET         */  bool hset(const RedisDBIdx& dbi,    const string& key, const string& filed, const string& value, int64_t& retval);
    /* HSETNX       */  bool hsetnx(const RedisDBIdx& dbi,  const string& key, const string& filed, const string& value);
    /* HVALS        */  bool hvals(const RedisDBIdx& dbi,   const string& key, VALUES& values);

    /* BLPOP        */  
    /* BRPOP        */  
    /* BRPOPLPUSH   */  
    /* LINDEX       */  bool lindex(const RedisDBIdx& dbi,    const string& key, const int64_t index, VALUE& value);
    /* LINSERT      */  bool linsert(const RedisDBIdx& dbi,  const string& key, const LMODEL mod, const string& pivot, const string& value, int64_t& retval);
    /* LLEN         */  bool llen(const RedisDBIdx& dbi,     const string& key, int64_t& len);
    /* LPOP         */  bool lpop(const RedisDBIdx& dbi,     const string& key, string& value);
    /* LPUSH        */  bool lpush(const RedisDBIdx& dbi,    const string& key, const VALUES& vValue, int64_t& length);
    /* LPUSHX       */  bool lpushx(const RedisDBIdx& dbi,   const string& key, const string& value, int64_t& length);
    /* LRANGE       */  bool lrange(const RedisDBIdx& dbi,   const string& key, const int64_t start, const int64_t end, ArrayReply& array);
    /* LREM         */  bool lrem(const RedisDBIdx& dbi,     const string& key,  const int count, const string& value, int64_t num);
    /* LSET         */  bool lset(const RedisDBIdx& dbi,     const string& key,  const int index, const string& value);
    /* LTRIM        */  bool ltrim(const RedisDBIdx& dbi,    const string& key,  const int start, const int end);
    /* RPOP         */  bool rpop(const RedisDBIdx& dbi,     const string& key, string& value);
    /* RPOPLPUSH    */  bool rpoplpush(const RedisDBIdx& dbi,const string& key_src, const string& key_dest, string& value);
    /* RPUSH        */  bool rpush(const RedisDBIdx& dbi,    const string& key, const VALUES& vValue, int64_t& length);
    /* RPUSHX       */  bool rpushx(const RedisDBIdx& dbi,   const string& key, const string& value, int64_t& length);



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
    /* SSCAN        */  
    /* SUNION       */  bool sunion(const DBIArray& dbi,      const KEYS& vkey, VALUES& vValue);
    /* SUNIONSTORE  */  bool sunionstore(const RedisDBIdx& dbi, const KEY& deskey, const DBIArray& vdbi, const KEYS& vkey, int64_t& count);

    /* ZADD             */  bool zadd(const RedisDBIdx& dbi,    const KEY& deskey,   const VALUES& vValues, int64_t& count);
    /* ZCARD            */  bool zscrad(const RedisDBIdx& dbi,  const string& key, int64_t& num);
    /* ZCOUNT           */
    /* ZINCRBY          */  bool zincrby(const RedisDBIdx& dbi, const string& key, const double &increment, const string& member, string& value );
    /* ZINTERSTORE      */  
    /* ZRANGE           */  bool zrange(const RedisDBIdx& dbi,  const string& key, int start, int end, VALUES& vValues, bool withscore=false);
    /* ZRANGEBYSCORE    */  
    /* ZRANK            */  bool zrank(const RedisDBIdx& dbi,   const string& key, const string& member, int64_t &rank);
    /* ZREM             */  bool zrem(const RedisDBIdx& dbi,    const KEY& key, const VALUES& vmembers, int64_t &num);
    /* ZREMRANGEBYRANK  */  bool zremrangebyrank(const RedisDBIdx& dbi,  const string& key, const int start, const int stop, int64_t& num);
    /* ZREMRANGEBYSCORE */  
    /* ZREVRANGE        */  bool zrevrange(const RedisDBIdx& dbi,  const string& key, int start, int end, VALUES& vValues, bool withscore=false);
    /* ZREVRANGEBYSCORE */  
    /* ZREVRANK         */  bool zrevrank(const RedisDBIdx& dbi,  const string& key, const string &member, int64_t& rank);
    /* ZSCAN            */  
    /* ZSCORE           */  bool zscore(const RedisDBIdx& dbi,  const string& key, const string &member, string& score);
    /* ZUNIONSTORE      */  

    /* PSUBSCRIBE   */
    /* PUBLISH      */
    /* PUBSUB       */
    /* PUNSUBSCRIBE */
    /* SUBSCRIBE    */
    /* UNSUBSCRIBE  */


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

public:

    bool command_bool(const RedisDBIdx& dbi,                       const char* cmd, ...);
    bool command_status(const RedisDBIdx& dbi,                     const char* cmd, ...);
    bool command_integer(const RedisDBIdx& dbi, int64_t &intval,   const char* cmd, ...);
    bool command_string(const RedisDBIdx& dbi,  string &data,      const char* cmd, ...);
    bool command_list(const RedisDBIdx& dbi,    VALUES &vValue,    const char* cmd, ...);
    bool command_array(const RedisDBIdx& dbi,   ArrayReply& array, const char* cmd, ...);

private:
    bool commandargv_bool(const RedisDBIdx& dbi,   const VDATA& vData);
    bool commandargv_status(const RedisDBIdx& dbi, const VDATA& vData);
    bool commandargv_array(const RedisDBIdx& dbi,  const VDATA& vDataIn, ArrayReply& array);
    bool commandargv_array(const RedisDBIdx& dbi,  const VDATA& vDataIn, VALUES& array);
    bool commandargv_integer(const RedisDBIdx& dbi,const VDATA& vDataIn, int64_t& retval);
private:
    RedisPool *mRedisPool;
};



#endif








