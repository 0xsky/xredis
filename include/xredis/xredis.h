#pragma once

#ifdef _MSC_VER
#include <winsock2.h>
#endif

#include <hiredis/hiredis.h>

#include "xRedisClient.h"
#include "xRedisClusterClient.h"
#include "xRedisPool.h"
