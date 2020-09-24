/*
 * ----------------------------------------------------------------------------
 * Copyright (c) 2013-2014, xSky <guozhw at gmail dot com>
 * All rights reserved.
 * Distributed under GPL license.
 * ----------------------------------------------------------------------------
 */

#include "xRedisClient.h"
using namespace xrc;

void xRedisClient::quit(){
	Release();
}


bool xRedisClient::echo(const SliceIndex& index, const std::string& str, std::string &value)
{
	if (0==str.length()) {
        return false;
    }
    SETDEFAULTIOTYPE(MASTER);
    return command_string(index, value, "echo %s", str.c_str());
}














