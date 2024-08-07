xRedis
======

xRedis 是一个C++开发的redis客户端，是对hiredis的C++封装，提供易用的redis命令操作接口.

***功能与特点：***
* 支持数据多节点分布存储，可自定义分片规则;
* 支持同时连接到每个分片的主从节点，支持主从读写分离;
* 支持对每个存储节点建立连接池;
* 支持同时连接多个数据分片集群;
* 支持连接到官方集群，支持单个节点和多个节点
    建立到每个节点的连接池，client端自动计算slot分布。
    支持自动计算节点索引位置,支持REDIS集群节点变化连接自动切换;
    当官方集群节点有添加/删除/slot分布变化时，到集群的连接池会自动更新。
* 提供简单易用的C++接口封装，已实现大部分REDIS命令;
* 只依赖hiredis库;
* 多线程安全
* 支持带密码连接;
* 支持linux、windows平台
 

### Dependencies

xredis 依赖 hiredis ,  在使用xRedis前需要安装[hiredis](https://github.com/redis/hiredis/)库

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
#使用 xRedisClusterClient类 访问 redis节点或是官方集群(redis cluster)

#include "xRedisClusterClient.h"
int main(int argc, char **argv) {
    xRedisClusterClient redisclient;
    # 连接到REDIS，建立大小为4的连接池，
    # 若此节点是官方集群的成员，则会自动对集群每个主节点建立大小为4的连接池。
    std::string passwd = "passwd123";
    bool bRet = redisclient.connect("127.0.0.1", 6379, passwd, 4);

    RedisResult result;
    redisclient.command(result, "set %s %s", "key", "hello");
    
    printf("type:%d integer:%lld str:%s \r\n",
        result.type(), result.integer(), result.str());

    while (true) {
        usleep(1000*1000*6);
        // 定时调用 keepalive 检测断线重连接，以及集群状态变化。
        redisclient.keepalive();
    }
     
    return 0;
}
```

更多使用示例
使用示例 [examples]目录(https://github.com/0xsky/xredis/blob/master/examples)

demo.cpp              使用xredisclient访问单个redis节点示例
demo_slice.cpp        使用xredisclient访问单组多分片(节点)redis分片集群示例
demo_multi_slice.cpp  使用xredisclient访问多组多分片(节点)redis分片集群示例
xredis-example.cpp    使用xredisclient 批量操作示例 
xredis-example-Consistent-hash xredisclient使用一致性hash，自定义数据分片存储demo

xRedisClusterClient_test.cpp  使用xRedisClusterClient实现简单的redis官方集群客户端示例
cluster-cli.cpp      使用 xRedisClusterClient 实现的连接redis集群的client，支持集群节增加/删除、slot分布变化时，自动切换。

/test/xredis-test.cpp   多个redis命令的使用示例

### 相关文档


### 计划
  添加 Redis 哨兵模式支持


##### xRedis 分片存储架构图
![xredis](doc/xredis_0.png)

[xRedis API](http://xredis.0xsky.com/)<br>
使用示例 [examples](https://github.com/0xsky/xredis/blob/master/examples) directory for some examples<br>
xRedis开源社区QQ群: 190107312<br>

作者: xSky<br>
博客: [xSky's Blog](https://0xsky.com/)<br>
xRedis QQ 群: 190107312
