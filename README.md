xRedis
======

Redis C++ client, support the data slice storage, support the connection pool

**Features:**
* data slice storage
* support Redis master slave connection, Support read/write separation
* connection pool
* simultaneously connected multiple data slice clusters  
* most REDIS commands have been implemented
* multi thread safety

### Dependencies

xredis requires hiredis only

### Install

First step install libhiredis, on a Debian system you can use:

```bash
sudo apt-get install libhiredis-dev
```

Then checkout the code and compile it
```bash
git clone https://github.com/0xsky/xredis
cd xredis
make
sudo make install
```

### Documentation
<p>[xRedis API Site](http://xredis.0xsky.com/) 
<p>See [examples](https://github.com/0xsky/xredis/blob/master/examples) directory for some examples
<p>xRedis开源社区QQ群: 190107312


<p><p>作者: xSky        
<p>博客: <a href="http://www.0xsky.com/">xSky's Blog</a>
<p>捐赠作者:[支付宝账号] guozhw@gmail.com

