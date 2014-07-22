xRedis
======

Redis C++ client, support the data slice storage, support the connection pool
xRedis 是一个C++开发的Redis客户端，支持Redis多节点分片，支持连接池。

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


