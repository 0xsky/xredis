/*
 * ----------------------------------------------------------------------------
 * Copyright (c) 2012-2013, xSky <guozhw at gmail dot com>
 * All rights reserved.
 * Distributed under GPL license.
 * ----------------------------------------------------------------------------
 */


#ifndef _XLOCK_H_
#define _XLOCK_H_

#ifdef WIN32
#include <Windows.h>
#else
#include <stdio.h>
#include <pthread.h>
#include <unistd.h>
#endif


class xLock {
private:
#ifdef WIN32
    CRITICAL_SECTION    mSection;
#else
    pthread_mutex_t     mMutex;
#endif
    
public:
    inline xLock() {
#ifdef WIN32
        InitializeCriticalSection(&mSection);
#else
        pthread_mutexattr_t attr;
        pthread_mutexattr_init(&attr);
        pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
        int ret = pthread_mutex_init(&mMutex, &attr);
        if(ret != 0 ){
            fprintf(stderr,"pthread_mutex_init error %d \n\r",ret);
        }
#endif
    };
    inline ~xLock() {
#ifdef WIN32
        DeleteCriticalSection(&mSection);
#else
        pthread_mutex_destroy(&mMutex);
#endif
    }
    inline void  Enter() {
#ifdef WIN32
        EnterCriticalSection(&mSection);
#else
        pthread_mutex_lock(&mMutex);
#endif
    }
    inline void Leave() {
#ifdef WIN32
        LeaveCriticalSection(&mSection);
#else
        pthread_mutex_unlock(&mMutex);
#endif
    };
};

class CLockUser {
public:
    inline  CLockUser(xLock& lock):mlock(lock) {
        mlock.Enter();
    };    
    inline  ~CLockUser(){
        mlock.Leave();
    }
private:
    xLock& mlock;
};

#define XLOCK(T) CLockUser lock(T)

#endif 

