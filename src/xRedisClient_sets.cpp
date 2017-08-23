/*
 * ----------------------------------------------------------------------------
 * Copyright (c) 2013-2014, xSky <guozhw at gmail dot com>
 * All rights reserved.
 * Distributed under GPL license.
 * ----------------------------------------------------------------------------
 */

#include "xRedisClient.h"

bool xRedisClient::sadd(const RedisDBIdx& dbi,     const string& key, const VALUES& vValue, int64_t& count){
    VDATA vCmdData;
    vCmdData.push_back("SADD");
    vCmdData.push_back(key);
    addparam(vCmdData, vValue);
    SETDEFAULTIOTYPE(MASTER);
    return commandargv_integer(dbi, vCmdData, count);
}

bool xRedisClient::scard(const RedisDBIdx& dbi,     const string& key, int64_t& count){
    if (0==key.length()) {
        return false;
    }
    SETDEFAULTIOTYPE(SLAVE);
    return command_integer(dbi, count, "SCARD %s", key.c_str());
}

bool xRedisClient::sdiff(const DBIArray& vdbi,     const KEYS& vkey, VALUES& sValue){
	size_t size = vkey.size();
    if (0 == size) {
        return false;
    }
    VALUES *setData = new VALUES[size];
    VALUES::iterator endpos;

    DBIArray::const_iterator iter_dbi = vdbi.begin();
    KEYS::const_iterator     iter_key = vkey.begin();
    int i=0;
    for (; iter_key!=vkey.end(); ++iter_key, ++iter_dbi, ++i) {
        const string &key = *iter_key;
        const RedisDBIdx &dbi = *iter_dbi;
        if (!smembers(dbi, key, setData[i])) {
            delete [] setData;
            return false;
        }
    }

    size_t n=0;
    while(n++<size-1) {
        endpos = set_difference( setData[n].begin(), setData[n].end(), setData[n+1].begin(), setData[n+1].end() , sValue.begin());
        sValue.resize( endpos - sValue.begin());
    }
    delete [] setData;
    return true;
}

bool xRedisClient::sdiffstore(const RedisDBIdx& dbi,  const KEY& destinationkey,  const DBIArray& vdbi, const KEYS& vkey, int64_t& count){
    VALUES sValue;
    if (!sdiff(vdbi, vkey, sValue)) {
       return false;
    }
    return sadd(dbi, destinationkey, sValue, count);
}

bool xRedisClient::sinter(const DBIArray& vdbi, const KEYS& vkey, VALUES& sValue){
    size_t size = vkey.size();
    VALUES *setData = new VALUES[size];
    VALUES::iterator endpos;

    DBIArray::const_iterator iter_dbi = vdbi.begin();
    KEYS::const_iterator     iter_key = vkey.begin();
    int i=0;
    for (; iter_key!=vkey.end(); ++iter_key, ++iter_dbi, ++i) {
        const string &key = *iter_key;
        const RedisDBIdx &dbi = *iter_dbi;
        if (!smembers(dbi, key, setData[i])) {
            delete [] setData;
            return false;
        }
    }

    size_t n=0;
    while(n++<size-1){
        endpos = set_intersection( setData[n].begin(), setData[n].end(), setData[n+1].begin(), setData[n+1].end() , sValue.begin());
        sValue.resize( endpos - sValue.begin());
    }
    delete [] setData;

    return true;
}

bool xRedisClient::sinterstore(const RedisDBIdx& des_dbi, const KEY& destinationkey, const DBIArray& vdbi, const KEYS& vkey, int64_t& count){
    VALUES sValue;
    if (!sinter(vdbi, vkey, sValue)) {
        return false;
    }
    return sadd(des_dbi, destinationkey, sValue, count);
}

bool xRedisClient::sismember(const RedisDBIdx& dbi,  const KEY& key,   const VALUE& member){
    if (0==key.length()) {
        return false;
    }
    return command_bool(dbi, "SISMEMBER %s %s", key.c_str(), member.c_str());
}

bool xRedisClient::smembers(const RedisDBIdx& dbi,  const KEY& key, VALUES& vValue){
    if (0==key.length()) {
        return false;
    }
    SETDEFAULTIOTYPE(SLAVE);
    return command_list(dbi, vValue, "SMEMBERS %s", key.c_str());
}

bool xRedisClient::smove(const RedisDBIdx& dbi,  const KEY& srckey, const KEY& deskey,  const VALUE& member){
    if (0==srckey.length()) {
        return false;
    }
    SETDEFAULTIOTYPE(MASTER);
    return command_bool(dbi, "SMOVE %s", srckey.c_str(), deskey.c_str(), member.c_str());
}

bool xRedisClient::spop(const RedisDBIdx& dbi,  const KEY& key, VALUE& member){
    if (0==key.length()) {
        return false;
    }
    SETDEFAULTIOTYPE(MASTER);
    return command_string(dbi, member, "SPOP %s", key.c_str());
}

bool xRedisClient::srandmember(const RedisDBIdx& dbi,  const KEY& key, VALUES& members, int count){
    if (0==key.length()) {
        return false;
    }
    SETDEFAULTIOTYPE(SLAVE);
    if (0==count) {
        return command_list(dbi, members, "SRANDMEMBER %s", key.c_str());
    }
    return command_list(dbi, members, "SRANDMEMBER %s %d", key.c_str(), count);
}

bool xRedisClient::srem(const RedisDBIdx& dbi,  const KEY& key, const VALUES& vmembers, int64_t& count){
    if (0==key.length()) {
        return false;
    }
    SETDEFAULTIOTYPE(MASTER);
    VDATA vCmdData;
    vCmdData.push_back("SREM");
    vCmdData.push_back(key);
    addparam(vCmdData, vmembers);
    return commandargv_integer(dbi, vCmdData, count);
}

bool xRedisClient::sscan(const RedisDBIdx& dbi, const std::string& key, int64_t &cursor, const char *pattern, 
    uint32_t count, ArrayReply& array, xRedisContext& ctx)
{
    return ScanFun("SSCAN", dbi, &key, cursor, pattern, count, array, ctx);
}

bool xRedisClient::sunion(const DBIArray& vdbi,     const KEYS& vkey, VALUES& sValue){
    size_t size = vkey.size();
    VALUES *setData = new VALUES[size];
    VALUES::iterator endpos;

    DBIArray::const_iterator iter_dbi = vdbi.begin();
    KEYS::const_iterator     iter_key = vkey.begin();
    int i=0;
    for (; iter_key!=vkey.end(); ++iter_key, ++iter_dbi, ++i) {
        const string &key = *iter_key;
        const RedisDBIdx &dbi = *iter_dbi;
        if (!smembers(dbi, key, setData[i])) {
            delete [] setData;
            return false;
        }
    }

    size_t n=0;
    while(n++<size-1) {
            endpos = set_union( setData[n].begin(), setData[n].end(), setData[n+1].begin(), setData[n+1].end() , sValue.begin());
        sValue.resize( endpos - sValue.begin());
    }
    delete [] setData;
    return true;
}

bool xRedisClient::sunionstore(const RedisDBIdx& dbi,  const KEY& deskey, const DBIArray& vdbi, const KEYS& vkey, int64_t& count){
    VALUES sValue;
    if (!dbi.mIOFlag) { SetIOtype(dbi, MASTER, true); }
    if (!sunion(vdbi, vkey, sValue)) {
        return false;
    }
    return sadd(dbi, deskey, sValue, count);
}


