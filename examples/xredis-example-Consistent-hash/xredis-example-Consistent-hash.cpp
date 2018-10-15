#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "xRedisClient.h"

// AP Hash Function
unsigned int APHash(const char *str) {
    unsigned int hash = 0;
    int i;

    for (i = 0; *str; i++)
    {
        if ((i & 1) == 0)
        {
            hash ^= ((hash << 7) ^ (*str++) ^ (hash >> 3));
        }
        else
        {
            hash ^= (~((hash << 11) ^ (*str++) ^ (hash >> 5)));
        }
    }

    return (hash & 0x7FFFFFFF);
}



#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <map>
#include <set>
#include <string>
#include <list>

typedef std::map<unsigned int, std::string> USMAP;
typedef std::set<std::string> SSET;
typedef std::map<unsigned int, SSET> USSMAP;
typedef std::list<std::string> SLIST;
typedef std::map<std::string, unsigned int> SUMAP;

typedef unsigned int(*HASHFUNC)(const char *str);


struct vnode_t {
    unsigned int id;
    const RedisNode *data;
};

typedef std::map<unsigned int, vnode_t*> VNODEMAP;


class ConHashIdx :public RedisDBIdx {
private:
    USMAP _node_info;            // nodeid to server
    USSMAP _node_slot;           // nodeid to keys   
    HASHFUNC _hash;              // hash function
    int _vnode_num;              // vnode number per server  

    VNODEMAP vNodeMap;

public:
    ConHashIdx(const RedisNode *rNode, int size, HASHFUNC hash, int vnode_num);
    ~ConHashIdx();
    void put_key(const std::string &key);
    void del_key(const std::string &key);
    std::string get_server(const std::string &key);

    unsigned int get_index(const char *key);
private:
    void _add_node(const RedisNode *rNode, int vnode_num);
    
};

void ConHashIdx::_add_node(const RedisNode *rNode, int vnode_num) {
    for (int i = 0; i < vnode_num; i++) {
        char buf[256];
        sprintf(buf, "%s-%010d-%u", rNode->host, i, time(NULL));
        unsigned int hval = _hash(buf);
        vnode_t *pVnode = new vnode_t;
        pVnode->id = hval;
        pVnode->data = rNode;
        printf("%u %s \r\n", pVnode->id, pVnode->data->host);
        vNodeMap.insert(make_pair(hval, pVnode));
    }
}

ConHashIdx::ConHashIdx(const RedisNode *rNode, int size, HASHFUNC hash, int vnode_num) {
    _hash = hash;
    _vnode_num = vnode_num;

    for (int i = 0; i < size; i++) {
        _add_node(&rNode[i], vnode_num);
    }
}

ConHashIdx::~ConHashIdx()
{

}

unsigned int ConHashIdx::get_index(const char *key)
{
    unsigned int hval = _hash(key);
    VNODEMAP::iterator iter = vNodeMap.upper_bound(hval);
    if (iter == vNodeMap.end() && 0 != vNodeMap.size()) {
        iter = vNodeMap.begin();
    }

    return iter->second->data->dbindex;
}

std::string ConHashIdx::get_server(const std::string &key)
{
    unsigned int hval = _hash(key.c_str());
    USMAP::iterator iter = _node_info.upper_bound(hval);
    if (iter == _node_info.end() && 0 != _node_info.size()) {
        iter = _node_info.begin();
    }

    return iter->second;
}

RedisNode RedisList1[3] =
{
    { 0, "127.0.0.1", 6379, "", 2, 5 },
    { 1, "127.0.0.2", 6379, "", 2, 5 },
    { 2, "127.0.0.3", 6379, "", 2, 5 }
};

RedisNode RedisList2[5] =
{
    { 0, "127.0.0.1", 6379, "", 2, 5 },
    { 1, "127.0.0.2", 6379, "", 2, 5 },
    { 2, "127.0.0.3", 6379, "", 2, 5 },
    { 3, "127.0.0.4", 6379, "", 2, 5 },
    { 4, "127.0.0.5", 6379, "", 2, 5 },
};

#define CACHE_TYPE_1 1
#define CACHE_TYPE_2 2

int main(int argc, char **argv) {

    xRedisClient xRedis;
    xRedis.Init(3);
    xRedis.ConnectRedisCache(RedisList1, 3, CACHE_TYPE_1);
    xRedis.ConnectRedisCache(RedisList2, 5, CACHE_TYPE_2);

    ConHashIdx cIdx(RedisList2, 5, APHash, 10);

    for (int i = 0; i < 100; ++i) {
        char szKey[256] = { 0 };
        sprintf(szKey, "test_%d_%u", i*8888, time(NULL));
        printf("%s index: %u \r\n", szKey, cIdx.get_index(szKey));
    }



    for (int n = 0; n < 1000; n++) {
        char szKey[256] = { 0 };
        sprintf(szKey, "test_%d", n);
        RedisDBIdx dbi(&xRedis);
        dbi.CreateDBIndex(szKey, APHash, CACHE_TYPE_1);
        bool bRet = xRedis.set(dbi, szKey, "hello redis!");
        if (!bRet){
            printf(" %s %s \n", szKey, dbi.GetErrInfo());
        }
    }

    for (int n = 0; n < 1000; n++) {
        char szKey[256] = { 0 };
        sprintf(szKey, "test_%d", n);
        RedisDBIdx dbi(&xRedis);
        dbi.CreateDBIndex(szKey, APHash, CACHE_TYPE_1);
        string strValue;
        xRedis.get(dbi, szKey, strValue);
        printf("%s \r\n", strValue.c_str());
    }

    int n = 10;
    while (n--) {
        xRedis.KeepAlive();
        usleep(1000 * 1000 * 10);
    }

    xRedis.release();

    return 0;
}


