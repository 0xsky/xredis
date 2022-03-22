
/*
 * ----------------------------------------------------------------------------
 * Copyright (c) 2013-2021, xSky <guozhw at gmail dot com>
 * All rights reserved.
 * Distributed under GPL license.
 * ----------------------------------------------------------------------------
 */

#ifndef _XREDIS_LOG_H_
#define _XREDIS_LOG_H_

#include "xRedisClient.h"
#include <stdint.h>
#include <stdio.h>

namespace xrc {

#define xredis_error(...) log_message(LOG_LEVEL_ERROR, __FUNCTION__, __LINE__, __VA_ARGS__)
#define xredis_warn(...) log_message(LOG_LEVEL_WARN, __FUNCTION__, __LINE__, __VA_ARGS__)
#define xredis_info(...) log_message(LOG_LEVEL_INFO, __FUNCTION__, __LINE__, __VA_ARGS__)
#define xredis_debug(...) log_message(LOG_LEVEL_DEBUG, __FUNCTION__, __LINE__, __VA_ARGS__)

#define DATA_BUF 4096

void log_message(uint32_t level, const char* function, int line, const char* fmt, ...);
void set_log_level(uint32_t level, void (*emit)(int level, const char* line));

} // namespace xrc

#endif
