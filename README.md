xRedis   
[![Build Status](https://travis-ci.org/freeeyes/PSS.svg?branch=master)](https://travis-ci.org/freeeyes/PSS)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0 )
[![GitHub version](https://badge.fury.io/gh/0xsky%2Fxredis.svg)](https://badge.fury.io/gh/0xsky%2Fxredis)
======

C++ Redis client, support the data slice storage, redis cluster, connection pool, read/write separation.

**Features:**
* Support multi-node distributed storage of data, can customize the sharding rules;
* Support to connect to master and slave nodes of each shard at the same time, support separation of master and slave reads and writes;
* Support connection pooling for each storage node;
* Support simultaneous connection of multiple data sharding clusters;
* Support to connect to the official cluster, support automatic calculation of node index position, 
  support Redis cluster node change  connection automatic switch;
  The connection pool to the cluster is automatically updated when the add/delete /slot distribution of the official cluster node changes.
* Provide easy to use C++ interface encapsulation, has implemented most of the Redis command;
* Only rely on the Hiredis library;
* Multi-thread safety
* Support password connection;
* Support Linux and Windows platforms

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
    std::string passwd = "passwd123";
    bool bRet = redisclient.ConnectRedis("127.0.0.1", 6379, passwd, 4);

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
<p>支持作者:
<img src='https://www.0xsky.com/images/donate.png' alt='捐赠作者' height='120px'>
Donate with PayPal guozhw@gmail.com 

Support xRedis: 
BTC: bc1q2c0fqc6c5h36t46n2cgz4kel4dutvjpzvta5ru
