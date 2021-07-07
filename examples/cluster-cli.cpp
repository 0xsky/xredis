/*
 * ----------------------------------------------------------------------------
 * Copyright (c) 2013-2021, xSky <guozhw at gmail dot com>
 * All rights reserved.
 * Distributed under GPL license.
 * ----------------------------------------------------------------------------
 */

/** \example cluster-cli.cpp
* This is an example used by XredisClusterClient
* Implemented a simple Redis console client
* Connecting a single Redis node and an official Redis cluster is supported
 */

#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <iostream>

using std::cin;
using std::cout;
using std::endl;

#include "xRedisClusterClient.h"
using namespace xrc;

xRedisClusterClient* redis = NULL;

static int str2Vect(const char* pSrc, std::vector<std::string>& vDest, const char* pSep = ",")
{
    if (NULL == pSrc) {
        return -1;
    }

    int iLen = strlen(pSrc);
    if (iLen == 0) {
        return -1;
    }

    char* pTmp = new char[iLen + 1];
    if (pTmp == NULL) {
        return -1;
    }

    memcpy(pTmp, pSrc, iLen);
    pTmp[iLen] = '\0';

    char* pCurr = strtok(pTmp, pSep);
    while (pCurr) {
        vDest.push_back(pCurr);
        pCurr = strtok(NULL, pSep);
    }

    delete[] pTmp;
    return 0;
}

void timer_handle(int sig)
{
    (void)sig;

    redis->keepalive();
}

void init_timer()
{
    struct sigevent evp;
    struct itimerspec ts;
    timer_t timer;

    memset(&evp, 0, sizeof(struct sigevent));
    evp.sigev_value.sival_ptr = &timer;
    evp.sigev_notify = SIGEV_SIGNAL;
    evp.sigev_signo = SIGUSR1;
    signal(SIGUSR1, timer_handle);

    int ret = timer_create(CLOCK_REALTIME, &evp, &timer);
    if (ret) {
        perror("timer_create");
        return;
    }

    ts.it_interval.tv_sec = 5;
    ts.it_interval.tv_nsec = 0;
    ts.it_value.tv_sec = 5;
    ts.it_value.tv_nsec = 0;

    ret = timer_settime(timer, 0, &ts, NULL);
    if (ret)
        perror("timer_settime");
}

void log_demo(int level, const char* line)
{
    printf("xRedis %u %s", level, line);
}

int main(int argc, char** argv)
{
    (void)argc;
    (void)argv;

    std::string pass;

    if (argc<3) {
        printf("Usage:%s host port pass \r\n", argv[0]);
        return -1;
    }  else if (4==argc) {
        pass = argv[3];
    }
    
    init_timer();
    xRedisClusterClient redisclient;
    //redisclient.setLogLevel(LOG_LEVEL_DEBUG, log_demo);
    redis = &redisclient;
    bool bRet = redisclient.connect(argv[1], atoi(argv[2]), pass, 4);

    if (!bRet) {
        fprintf(stderr, "Connect to %s:%s error\r\n", argv[0], argv[1]);
        return -1;
    }

    std::string strInput;
    while (true) {
        cout << "\033[32mxRedis> \033[0m";
        getline(cin, strInput);
        if (!cin)
            return 0;

        if (strInput.length() < 1) {
            cout << "input again" << endl;
        } else {
            RedisResult result;
            VString vDataIn;

            str2Vect(strInput.c_str(), vDataIn, " ");
            redisclient.commandArgv(vDataIn, result);

            switch (result.type()) {
            case REDIS_REPLY_INTEGER: {
                printf("%lld \r\n", result.integer());
                break;
            }
            case REDIS_REPLY_NIL: {
                printf("%lld %s \r\n", result.integer(), result.str());
                break;
            }
            case REDIS_REPLY_STATUS: {
                printf("%s \r\n", result.str());
                break;
            }
            case REDIS_REPLY_ERROR: {
                printf("%s \r\n", result.str());
                break;
            }
            case REDIS_REPLY_STRING: {
                printf("%s \r\n", result.str());
                break;
            }
            case REDIS_REPLY_ARRAY: {
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
