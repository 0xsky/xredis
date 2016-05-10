xRedis
======

C++ Redis client, support the data slice storage, connection pool, read/write separation.

**Features:**
* data slice storage
* support Redis master slave connection, Support read/write separation
* connection pool
* simultaneously connected multiple data slice groups  
* most Redis commands have been implemented
* multi thread safety

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
