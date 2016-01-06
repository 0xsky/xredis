xRedis
======

Redis C++ client, support the data slice storage, support the connection pool

xRedis 是一个C++开发的redis客户端，是对hiredis的C++封装，提供REDIS易用的命令操作接口.
支持数据多节点分布存储;
支持同时连接到Redis主从节点，支持各命令操作读写分离;
支持连接池;
能同时连接多个集群;
已实现大多数REDIS命令;
只依赖hiredis库;
多线程安全;


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

### 相关文档

See [examples](https://github.com/0xsky/xredis/blob/master/examples) directory for some examples


