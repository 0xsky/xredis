
#include "xRedisClient.h"
#include <sstream>


bool xRedisClient::lindex(const RedisDBIdx& dbi,    const string& key, const int64_t index, VALUE& value){
    return command_string(dbi, value, "LINDEX %s %lld", key.c_str(), index);
}

bool xRedisClient::linsert(const RedisDBIdx& dbi,    const string& key, const LMODEL mod, const string& pivot, const string& value, int64_t& retval){
    static const char *lmodel[2]= {"BEFORE","AFTER"};
    return command_integer(dbi, retval, "LINSERT %s %s %s %s", key.c_str(), lmodel[mod], pivot.c_str(), value.c_str());
}

bool xRedisClient::llen(const RedisDBIdx& dbi,    const string& key, int64_t& retval){
    return command_integer(dbi, retval, "LLEN %s", key.c_str());
}

bool xRedisClient::lpop(const RedisDBIdx& dbi,    const string& key, string& value){
    return command_string(dbi, value, "LPOP %s", key.c_str());
}

bool xRedisClient::lpush(const RedisDBIdx& dbi,    const string& key, const VALUES& vValue, int64_t& length){
    VDATA vCmdData;
    vCmdData.push_back("lpush");
    vCmdData.push_back(key);
    addparam(vCmdData, vValue);
    return commandargv_integer(dbi, vCmdData, length);
}

bool xRedisClient::lrange(const RedisDBIdx& dbi,    const string& key, const int64_t start, const int64_t end, ArrayReply& array){
    return command_array(dbi, array, "LRANGE %s", key.c_str(), start, end);
}

bool xRedisClient::lrem(const RedisDBIdx& dbi, const string& key, const int count, const string& value, int64_t num){
    return command_integer(dbi, num, "LREM %s %d %s", key.c_str(), count, value.c_str());
}

bool xRedisClient::lset(const RedisDBIdx& dbi, const string& key, const int index, const string& value){
    return command_status(dbi, "LSET %s %d %s", key.c_str(), index, value.c_str());
}

bool xRedisClient::ltrim(const RedisDBIdx& dbi, const string& key, const int start, const int end){
    return command_status(dbi, "ltrim %s %d %d", key.c_str(), start, end);
}

bool xRedisClient::rpop(const RedisDBIdx& dbi,    const string& key, string& value){
    return command_string(dbi, value, "LPOP %s", key.c_str());
}

bool xRedisClient::rpoplpush(const RedisDBIdx& dbi,    const string& key_src, const string& key_dest, string& value){
    return command_string(dbi, value, "RPOPLPUSH %s %s", key_src.c_str(), key_dest.c_str());
}

bool xRedisClient::rpush(const RedisDBIdx& dbi,    const string& key, const VALUES& vValue, int64_t& length){
    VDATA vCmdData;
    vCmdData.push_back("rpush");
    vCmdData.push_back(key);
    addparam(vCmdData, vValue);
    return commandargv_integer(dbi, vCmdData, length);
}

bool xRedisClient::rpushx(const RedisDBIdx& dbi,   const string& key, const string& value, int64_t& length){
    return command_integer(dbi, length, "RPUSHX %s %s", key.c_str(), value.c_str());
}

