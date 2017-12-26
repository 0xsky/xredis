/*
 * ----------------------------------------------------------------------------
 * Copyright (c) 2013-2014, xSky <guozhw at gmail dot com>
 * All rights reserved.
 * Distributed under GPL license.
 * ----------------------------------------------------------------------------
 */

#include "hiredis.h"
#include "xRedisClient.h"
#include "xRedisPool.h"
#include <sstream>

bool xRedisClient::psetex(const RedisDBIdx& dbi,    const string& key,  int milliseconds, const string& value) {
    SETDEFAULTIOTYPE(MASTER);
    return command_bool(dbi, "PSETEX %s %d %s", key.c_str(), milliseconds, value.c_str());
}

bool xRedisClient::append(const RedisDBIdx& dbi,    const string& key,  const string& value) {
    VDATA vCmdData;
    vCmdData.push_back("APPEND");
    vCmdData.push_back(key);
    vCmdData.push_back(value);
    SETDEFAULTIOTYPE(MASTER);
    return commandargv_status(dbi, vCmdData);
}

bool xRedisClient::set(const RedisDBIdx& dbi,    const string& key,  const string& value) {
    VDATA vCmdData;
    vCmdData.push_back("SET");
    vCmdData.push_back(key);
    vCmdData.push_back(value);
    SETDEFAULTIOTYPE(MASTER);
    return commandargv_status(dbi, vCmdData);
}

bool xRedisClient::set(const RedisDBIdx& dbi, const string& key, const string& value, SETPXEX pxex, int expiretime, SETNXXX nxxx) {
    static const char* pXflag[]={"px","ex","nx","xx"};
    SETDEFAULTIOTYPE(MASTER);

    VDATA vCmdData;
    vCmdData.push_back("SET");
    vCmdData.push_back(key);
    vCmdData.push_back(value);

    if (pxex>0) {
        vCmdData.push_back((pxex == PX) ? pXflag[0] : pXflag[1]);
        vCmdData.push_back(toString(expiretime));
    }

    if (nxxx>0){
        vCmdData.push_back((nxxx == NX) ? pXflag[2] : pXflag[3]);
    }

    return commandargv_status(dbi, vCmdData);
}

bool xRedisClient::set(const RedisDBIdx& dbi, const string& key, const char *value, int len, int second) {
    SETDEFAULTIOTYPE(MASTER);
    if (0==second) {
        return command_bool(dbi, "set %s %b", key.c_str(), value, len);
    } else {
        return command_bool(dbi, "set %s %b EX %d", key.c_str(), value, len, second);
    }
}

bool xRedisClient::setbit(const RedisDBIdx& dbi, const string& key,  int offset, int64_t newbitValue, int64_t oldbitValue) {
    SETDEFAULTIOTYPE(MASTER);
    return command_integer(dbi, oldbitValue, "SETBIT %s %d %lld", key.c_str(), offset, newbitValue);
}

bool xRedisClient::get(const RedisDBIdx& dbi, const string& key,  string& value) {
    SETDEFAULTIOTYPE(SLAVE);
    return command_string(dbi, value, "GET %s", key.c_str());
}

bool xRedisClient::getbit( const RedisDBIdx& dbi, const string& key, int& offset, int& bit ) {
    SETDEFAULTIOTYPE(SLAVE);
    int64_t intval = 0;
    bool bRet = command_integer(dbi, intval, "GETBIT %s %d", key.c_str(), offset);
    bit = (int)intval;
    return bRet;
}

bool xRedisClient::getrange(const RedisDBIdx& dbi,const string& key,  int start, int end, string& out) {
    SETDEFAULTIOTYPE(SLAVE);
    return command_string(dbi, out, "GETRANGE %s %d %d", key.c_str(), start, end);
}

bool xRedisClient::getset(const RedisDBIdx& dbi, const string& key,  const string& newValue, string& oldValue) {
    SETDEFAULTIOTYPE(MASTER);
    return command_string(dbi, oldValue, "GETSET %s %s", key.c_str(), newValue.c_str());
}

bool xRedisClient::mget(const DBIArray &vdbi,   const KEYS &  keys, ReplyData& vDdata) {
    bool bRet = false;
    size_t n = vdbi.size();
    if (n!=keys.size()) {
        return bRet;
    }

    DataItem item;
    DBIArray::const_iterator iter_dbi = vdbi.begin();
    KEYS::const_iterator iter_key = keys.begin();
    for (;iter_key!=keys.end();++iter_key, ++iter_dbi) {
        const RedisDBIdx& dbi = *iter_dbi;
        SETDEFAULTIOTYPE(SLAVE);
        const string &key = *iter_key;
        if (key.length()>0) {
            bool ret = command_string(*iter_dbi, item.str, "GET %s", key.c_str());
            if (!ret) {
                item.type = REDIS_REPLY_NIL;
                item.str  = "";
            } else {
                item.type = REDIS_REPLY_STRING;
                bRet = true;
            }
            vDdata.push_back(item);
        }
    }
    return bRet;
}

bool xRedisClient::mset(const DBIArray& vdbi, const VDATA& vData) {
    DBIArray::const_iterator iter_dbi = vdbi.begin();
    VDATA::const_iterator iter_data = vData.begin();
    for (; iter_data != vData.end(); iter_dbi++) {
        const string &key = (*iter_data++);
        const string &value = (*iter_data++);
        const RedisDBIdx& dbi = *iter_dbi;
        SETDEFAULTIOTYPE(SLAVE);
        command_status(dbi, "SET %s %s", key.c_str(), value.c_str());
    }
    return true;
}

bool xRedisClient::setex(const RedisDBIdx& dbi,    const string& key,  int seconds, const string& value) {
    VDATA vCmdData;

    vCmdData.push_back("SETEX");
    vCmdData.push_back(key);
    vCmdData.push_back(toString(seconds));
    vCmdData.push_back(value);
    SETDEFAULTIOTYPE(MASTER);
    return commandargv_status(dbi, vCmdData);
}

bool xRedisClient::setnx(const RedisDBIdx& dbi,  const string& key,  const string& value) {
    VDATA vCmdData;
    vCmdData.push_back("SETNX");
    vCmdData.push_back(key);
    vCmdData.push_back(value);
    SETDEFAULTIOTYPE(MASTER);
    return commandargv_bool(dbi, vCmdData);
}

bool xRedisClient::setrange(const RedisDBIdx& dbi,const string& key,  int offset, const string& value, int& length) {
    int64_t intval = 0;
    SETDEFAULTIOTYPE(MASTER);
    bool bRet = command_integer(dbi, intval, "setrange %s %d %s", key.c_str(), offset, value.c_str());
    length = (int)intval;
    return bRet;
}

bool xRedisClient::strlen(const RedisDBIdx& dbi,const string& key, int& length) {
    int64_t intval = 0;
    SETDEFAULTIOTYPE(SLAVE);
    bool bRet = command_integer(dbi, intval, "STRLEN %s", key.c_str());
    length = (int)intval;
    return bRet;
}

bool xRedisClient::incr(const RedisDBIdx& dbi,   const string& key, int64_t& result) {
    SETDEFAULTIOTYPE(MASTER);
    return command_integer(dbi, result, "INCR %s", key.c_str());
}

bool xRedisClient::incrby(const RedisDBIdx& dbi, const string& key, int by, int64_t& result) {
    SETDEFAULTIOTYPE(MASTER);
    return command_integer(dbi, result, "INCRBY %s %d", key.c_str(), by);
}

bool xRedisClient::bitcount(const RedisDBIdx& dbi,  const string& key, int& count, int start, int end) {
    int64_t intval = 0;
    bool bRet = false;
    SETDEFAULTIOTYPE(SLAVE);
    if ( (start!=0)||(end!=0) ) {
        bRet = command_integer(dbi, intval, "bitcount %s %d %d", key.c_str(), start, end);
    } else {
        bRet =  command_integer(dbi, intval, "bitcount %s", key.c_str());
    }
    count = (int)intval;
    return bRet;
}

//// 这个实现有问题
//bool xRedisClient::bitop(const RedisDBIdx& dbi, const BITOP operation, const string& destkey, const KEYS& keys, int& lenght) {
//    static const char *op_cmd[4]= {"AND","OR","XOR","NOT"};
//    VDATA vCmdData;
//    int64_t intval = 0;
//    vCmdData.push_back("bitop");
//    vCmdData.push_back(op_cmd[operation]);
//    vCmdData.push_back(destkey);
//    addparam(vCmdData, keys);
//    SETDEFAULTIOTYPE(MASTER);
//    bool bRet = commandargv_integer(dbi, vCmdData, intval);
//    lenght = (int)intval;
//    return bRet;
//}

bool xRedisClient::bitpos(const RedisDBIdx& dbi, const string& key, int bit, int64_t& pos, int start, int end) {
    SETDEFAULTIOTYPE(SLAVE);
    if ( (start!=0)||(end!=0) ) {
        return command_integer(dbi, pos, "BITPOS %s %d %d %d", key.c_str(), bit, start, end);
    }
    return command_integer(dbi, pos, "BITPOS %s %d", key.c_str(), bit);
}

bool xRedisClient::decr(const RedisDBIdx& dbi,   const string& key, int64_t& result) {
    SETDEFAULTIOTYPE(MASTER);
    return command_integer(dbi,result,"decr %s", key.c_str());
}

bool xRedisClient::decrby(const RedisDBIdx& dbi, const string& key, int by, int64_t& result) {
    SETDEFAULTIOTYPE(MASTER);
    return command_integer(dbi, result, "decrby %s %d", key.c_str(), by);
}


















