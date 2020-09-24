xRedis
======

Redis C++ client, support the data slice storage, support the connection pool

xRedis 是一个C++开发的redis客户端，是对hiredis的C++封装，提供易用的redis命令操作接口.

***功能与特点：***
* 支持数据多节点分布存储，可自定义分片规则;
* 支持连接到官方集群，支持自动计算节点索引位置;
* 支持同时连接到每个分片的主从节点，支持主从读写分离;
* 支持对每个存储节点建立连接池;
* 支持同时连接多个数据分片集群;
* 提供简单易用的C++接口封装，已实现大部分REDIS命令;
* 只依赖hiredis库;
* 多线程安全
 

### Dependencies

xredis 依赖 hiredis ,  在使用xRedis前需要安装hiredis库

### Install

第一步 安装libhiredis
 在Debian系统上:
```bash
sudo apt-get install libhiredis-dev
```

xRedis源码安装
```bash
git clone https://github.com/0xsky/xredis
cd xredis
make
sudo make install
```
使用说明
```C++
#使用 xRedisClusterClient类 访问 redis节点或是官方集群

#include "xRedisClusterClient.h"
int main(int argc, char **argv) {
    xRedisClusterClient redisclient;
    # 连接到REDIS，建立大小为4的连接池，
    # 若此节点是官方集群的成员，则会自动对集群每个主节点建立大小为4的连接池。
    bool bRet = redisclient.ConnectRedis("127.0.0.1", 6379, 4);

    RedisResult result;
    redisclient.RedisCommand(result, "set %s %s", "key", "hello");
    
    printf("type:%d integer:%lld str:%s \r\n",
        result.type(), result.integer(), result.str());

    return 0;
}
```

### 相关文档
##### xRedis 分片存储架构图
![xredis](http://xredis.0xsky.com/pic/xredis_0.png)
<p>[xRedis API](http://xredis.0xsky.com/) 
<p>使用示例 [examples](https://github.com/0xsky/xredis/blob/master/examples) directory for some examples
<p>xRedis开源社区QQ群: 190107312

<p><p>作者: xSky        
<p>博客: <a href="http://www.0xsky.com/">xSky's Blog</a>
<p>xRedis QQ 群: 190107312 
<p>支持作者:
<img src='https://www.0xsky.com/images/donate.png' alt='捐赠作者' height='120px'>
