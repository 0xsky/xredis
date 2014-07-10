
#include "xRedisClient.h"
#include <sstream>

int  xRedisClient::del(const RedisDBIdx& dbi,    const string& key) {
    return command_bool(dbi, "DEL %s", key.c_str());
}

int  xRedisClient::del(const RedisDBIdx& dbi,    const KEYS &  vkey, int64_t& count) {
    VDATA vCmdData;
    vCmdData.push_back("DEL");
    addparam(vCmdData, vkey);
    return commandargv_integer(dbi, vCmdData, count);
}

bool xRedisClient::exists(const RedisDBIdx& dbi, const string& key) {
    return command_bool(dbi, "EXISTS %s", key.c_str());
}

bool xRedisClient::expire(const RedisDBIdx& dbi, const string& key, const unsigned int second) {
    return command_bool(dbi, "EXPIRE %s %u", key.c_str(), second);
}

bool xRedisClient::expireat(const RedisDBIdx& dbi, const string& key, const unsigned int timestamp) {
    return command_bool(dbi, "EXPIREAT %s %u", key.c_str(), timestamp);
}

bool xRedisClient::persist(const RedisDBIdx& dbi, const string& key) {
    return command_bool(dbi, "PERSIST %s %u", key.c_str());
}

bool xRedisClient::pexpire(const RedisDBIdx& dbi, const string& key, const unsigned int milliseconds) {
    return command_bool(dbi, "PEXPIRE %s %u", key.c_str(), milliseconds);
}

bool xRedisClient::pexpireat(const RedisDBIdx& dbi, const string& key, const unsigned int millisecondstimestamp) {
    return command_bool(dbi, "PEXPIREAT %s %u", key.c_str(), millisecondstimestamp);
}

bool xRedisClient::pttl(const RedisDBIdx& dbi, const string& key, int64_t &milliseconds) {
    return command_integer(dbi, milliseconds, "PEXPIREAT %s", key.c_str());
}

bool xRedisClient::ttl(const RedisDBIdx& dbi, const string& key, int64_t &seconds) {
    return command_integer(dbi, seconds, "TTL %s", key.c_str());
}

bool xRedisClient::randomkey(const RedisDBIdx& dbi, KEY& key){
    return command_string(dbi, key, "RANDOMKEY");
}







