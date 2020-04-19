/*
 * ----------------------------------------------------------------------------
 * Copyright (c) 2013-2014, xSky <guozhw at gmail dot com>
 * All rights reserved.
 * Distributed under GPL license.
 * ----------------------------------------------------------------------------
 */
#include <sstream>
#include <fmt/printf.h>
#include <fmt/format.h>
#include "xredis.h"

using namespace xrc;

void xRedisClient::quit()
{
    Release();
}

bool xRedisClient::echo(const RedisDBIdx &dbi, const std::string &str, std::string &value)
{
    if (0 == str.length())
    {
        return false;
    }
    SETDEFAULTIOTYPE(MASTER);
    return command_string(dbi, value, fmt::format("echo {}").c_str());
}
