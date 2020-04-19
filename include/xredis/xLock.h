/*
 * ----------------------------------------------------------------------------
 * Copyright (c) 2012-2013, xSky <guozhw at gmail dot com>
 * All rights reserved.
 * Distributed under GPL license.
 * ----------------------------------------------------------------------------
 */

#pragma once

#include <mutex>

class xLock
{
private:
    std::recursive_mutex mMutex;

public:
    inline void Enter()
    {
        mMutex.lock();
    }
    inline void Leave()
    {
        mMutex.unlock();
    };
};

class CLockUser
{
public:
    inline CLockUser(xLock &lock) : mlock(lock)
    {
        mlock.Enter();
    };
    inline ~CLockUser()
    {
        mlock.Leave();
    }

private:
    xLock &mlock;
};

#define XLOCK(T) CLockUser lock(T)
