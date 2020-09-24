#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <iostream>
#include "xRedisClusterClient.h"

using std::cout;
using std::endl;
using std::cin;

/*
    这是连接redis官方集群客户端的一个示例
*/

int main(int argc, char **argv) {
    (void)argc;(void)argv;
    
    xRedisClusterClient ClusterClient;
    
    bool bRet = ClusterClient.ConnectRedis(argv[1], atoi(argv[2]), 2);
    if(!bRet) {
        return -1;
    }

    std::string  strInput;
    while (true) {
        cout << "\033[32mxRedis> \033[0m";
        getline(cin, strInput);
        if (!cin)
            return 0;

        if (strInput.length() < 1) {
            cout << "input again" << endl;
        } else {
            RedisResult result;
            VSTRING vDataIn;

            xRedisClusterClient::str2Vect(strInput.c_str(), vDataIn, " ");
            ClusterClient.RedisCommandArgv(vDataIn, result);

            switch (result.type()){
            case REDIS_REPLY_INTEGER:{ printf("%lld \r\n", result.integer()); break;}
            case REDIS_REPLY_NIL:    { printf("%lld %s \r\n", result.integer(), result.str()); break; }
            case REDIS_REPLY_STATUS: { printf("%s \r\n", result.str()); break; }
            case REDIS_REPLY_ERROR:  { printf("%s \r\n", result.str()); break; }
            case REDIS_REPLY_STRING: { printf("%s \r\n", result.str()); break; }
            case REDIS_REPLY_ARRAY:  {
                for (size_t i = 0; i < result.elements(); ++i) {
                    RedisResult::RedisReply reply = result.element(i);
                    printf("type:%d integer:%lld str:%s \r\n",
                        reply.type(), reply.integer(), reply.str());
                }
                break;
            }
            }
        }
    }

    return 0;
}
