
#include "xRedisClient.h"
#include <sstream>


bool xRedisClient::sadd(const RedisDBIdx& dbi,     const string& key, const VALUES& vValue, int64_t& count){
    VDATA vCmdData;
    vCmdData.push_back("SADD");
    vCmdData.push_back(key);
    addparam(vCmdData, vValue);
    return commandargv_integer(dbi, vCmdData, count);
}

bool xRedisClient::scrad(const RedisDBIdx& dbi,     const string& key, int64_t& count){
    return command_integer(dbi, count, "SCRAD %s", key.c_str());
}

bool xRedisClient::sdiff(const RedisDBIdx& dbi,     const KEYS& vkey, VALUES& vValue){
    VDATA vCmdData;
    vCmdData.push_back("SDIFF");
    addparam(vCmdData, vkey);
    return commandargv_array(dbi, vCmdData, vValue);
}

bool xRedisClient::sdiffstore(const RedisDBIdx& dbi,  const KEY& destinationkey,   const KEYS& vkey, int64_t& count){
    VDATA vCmdData;
    vCmdData.push_back("SDIFFSTORE");
    vCmdData.push_back(destinationkey);
    addparam(vCmdData, vkey);
    return commandargv_integer(dbi, vCmdData, count);
}

bool xRedisClient::sinter(const RedisDBIdx& dbi, const KEYS& vkey, VALUES& vValue){
    VDATA vCmdData;
    vCmdData.push_back("SINTER");
    addparam(vCmdData, vkey);
    return commandargv_array(dbi, vCmdData, vValue);
}

bool xRedisClient::sinterstore(const RedisDBIdx& dbi, const KEY& destinationkey, const KEYS& vkey, int64_t& count){
    VDATA vCmdData;
    vCmdData.push_back("SINTERSTORE");
    vCmdData.push_back(destinationkey);
    addparam(vCmdData, vkey);
    return commandargv_integer(dbi, vCmdData, count);
}

bool xRedisClient::sismember(const RedisDBIdx& dbi,  const KEY& key,   const VALUE& member){
    return command_bool(dbi, "SISMEMBER %s %s", key.c_str(), member.c_str());
}

bool xRedisClient::smember(const RedisDBIdx& dbi,  const KEY& key, VALUES& vValue){
    return command_list(dbi, vValue, "SMEMBER %s", key.c_str());
}

bool xRedisClient::smove(const RedisDBIdx& dbi,  const KEY& srckey, const KEY& deskey,  const VALUE& member){
    return command_bool(dbi, "SMOVE %s", srckey.c_str(), deskey.c_str(), member.c_str());
}

bool xRedisClient::spop(const RedisDBIdx& dbi,  const KEY& key, VALUE& member){
    return command_string(dbi, member, "SPOP %s", key.c_str());
}

bool xRedisClient::srandmember(const RedisDBIdx& dbi,  const KEY& key, VALUES& members, int count){
    if (0==count) {
        return command_list(dbi, members, "SRANDMEMBER %s", key.c_str());
    }
    return command_list(dbi, members, "SRANDMEMBER %s %d", key.c_str(), count);
}

bool xRedisClient::srem(const RedisDBIdx& dbi,  const KEY& key, const VALUES& vmembers, int64_t& count){
    VDATA vCmdData;
    vCmdData.push_back("SREM");
    vCmdData.push_back(key);
    addparam(vCmdData, vmembers);
    return commandargv_integer(dbi, vCmdData, count);
}

bool xRedisClient::sunion(const RedisDBIdx& dbi,     const KEYS& vkey, VALUES& vValue){
    VDATA vCmdData;
    vCmdData.push_back("SREM");
    addparam(vCmdData, vkey);
    return commandargv_array(dbi, vCmdData, vValue);
}

bool xRedisClient::sunionstore(const RedisDBIdx& dbi,  const KEY& deskey,   const KEYS& vkey, int64_t& count){
    VDATA vCmdData;
    vCmdData.push_back("SUNIONSTORE");
    vCmdData.push_back(deskey);
    addparam(vCmdData, vkey);
    return commandargv_integer(dbi, vCmdData, count);
}


