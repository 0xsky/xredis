/*
 * ----------------------------------------------------------------------------
 * Copyright (c) 2013-2014, xSky <guozhw at gmail dot com>
 * All rights reserved.
 * Distributed under GPL license.
 * ----------------------------------------------------------------------------
 */

#include "xRedisClient.h"
#include <sstream>

bool xRedisClient::auth(const RedisDBIdx& dbi, const string& pass){
    return command_bool(dbi, "AUTH %s", pass.c_str());
}














