#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <iostream>
#include "xRedisClusterClient.h"

using std::cout;
using std::endl;
using std::cin;

int main(int argc, char **argv) {

    xRedisClusterClient redisclient;
    
    bool bRet = redisclient.ConnectRedis(argv[1], atoi(argv[2]), 2);
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
            xlog("cmd:%s size:%u\r\n", strInput.c_str(), vDataIn.size());

            redisclient.RedisCommandArgv(vDataIn, result);

            xlog("type:%d integer:%lld str:%s \r\n",
                result.type(), result.integer(), result.str());

            for (size_t i = 0; i < result.elements(); ++i) {
                RedisResult::RedisReply reply = result.element(i);
                printf("type:%d integer:%lld str:%s \r\n",
                    reply.type(), reply.integer(), reply.str());
            }
        }

    }

    return 0;
}
