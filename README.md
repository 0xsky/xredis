xRedis   
[![Build Status](https://travis-ci.org/freeeyes/PSS.svg?branch=master)](https://travis-ci.org/freeeyes/PSS)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0 )
[![GitHub version](https://badge.fury.io/gh/0xsky%2Fxredis.svg)](https://badge.fury.io/gh/0xsky%2Fxredis)
======

C++ Redis client, support the data slice storage, redis cluster, connection pool, read/write separation.

**Features:**
* data slice storage
* support Redis master slave connection, Support read/write separation
* suppert redis cluster
* connection pool
* simultaneously connected multiple data slice groups  
* most Redis commands have been implemented
* multi thread safety
* suport linux and windows

中文版说明文档点[这里](https://github.com/0xsky/xredis/blob/master/README-cn.md)

### Dependencies

xredis requires hiredis only

### Install

First step install libhiredis, on a Debian system you can use:

```bash
sudo apt-get install libhiredis-dev
```

on centos/redhat/fedora system you can use:
```bash
sudo yum install hiredis-devel
```

Then checkout the code and compile it
```bash
git clone https://github.com/0xsky/xredis
cd xredis
make
sudo make install
```

Usage
```C++
#Accessing redis or  redis Cluster using the xRedisClusterClient class

#include "xRedisClusterClient.h"
int main(int argc, char **argv) {
    xRedisClusterClient redisclient;
    # Connect to REDIS and establish a connection pool 
    # If this node is a member of the REDIS cluster, 
    # a connection pool is automatically established for each primary node in the cluster.
    bool bRet = redisclient.ConnectRedis("127.0.0.1", 6379, 4);

    RedisResult result;
    redisclient.RedisCommand(result, "set %s %s", "key", "hello");
    
    printf("type:%d integer:%lld str:%s \r\n",
        result.type(), result.integer(), result.str());

    return 0;
}
```


### Documentation
![xredis](http://xredis.0xsky.com/pic/xredis_0.png)
<p>[xRedis API Site](http://xredis.0xsky.com/) 
<p>See [examples](https://github.com/0xsky/xredis/blob/master/examples) directory for some examples

<p>blog: <a href="http://www.0xsky.com/">xSky's Blog</a>
<p>xRedis QQ Group: 190107312
<img src='https://www.0xsky.com/images/donate.png' alt='捐赠作者' height='120px'>


