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

### Documentation
![xredis](http://xredis.0xsky.com/pic/xredis_0.png)
<p>[xRedis API Site](http://xredis.0xsky.com/) 
<p>See [examples](https://github.com/0xsky/xredis/blob/master/examples) directory for some examples

<p>blog: <a href="http://www.0xsky.com/">xSky's Blog</a>
<p>xRedis QQ Group: 190107312
  <p>[Donate]捐赠作者(https://github.com/0xsky/xredis/blob/master/donate.jpg) 

![](./donate.jpg =100x100)

