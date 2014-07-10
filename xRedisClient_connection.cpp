
#include "xRedisClient.h"
#include <sstream>

bool xRedisClient::auth(const RedisDBIdx& dbi, const string& pass){
    return command_bool(dbi, "AUTH %s", pass.c_str());
}














