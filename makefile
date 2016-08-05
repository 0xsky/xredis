# xredis Makefile
# Copyright (C) 2010-2011 Salvatore Sanfilippo <antirez at gmail dot com>
# Copyright (C) 2010-2011 Pieter Noordhuis <pcnoordhuis at gmail dot com>
# Copyright (C) 2013-2016 xsky <guozhw at gmail dot com>
# This file is released under the BSD license, see the COPYING file

OBJ=src/xRedisClient.o src/xRedisClient_keys.o src/xRedisClient_sets.o src/xRedisClient_strings.o src/xRedisClient_pubsub.o \
src/xRedisClient_connection.o src/xRedisClient_hashs.o src/xRedisClient_lists.o src/xRedisClient_sortedsets.o src/xRedisPool.o
EXAMPLES=xredis-example 
TESTS=xredis-test
LIBNAME=libxredis

XREDIS_MAJOR=0
XREDIS_MINOR=11

# Fallback to gcc when $CC is not in $PATH.
CC:=g++
OPTIMIZATION?=-O3
WARNINGS=-Wall -W -Wwrite-strings
DEBUG?= -g -ggdb
REAL_CFLAGS=$(OPTIMIZATION) -fPIC $(CFLAGS) $(WARNINGS) $(DEBUG) $(ARCH)
REAL_LDFLAGS=$(LDFLAGS) $(ARCH)

DYLIBSUFFIX=so
STLIBSUFFIX=a
DYLIB_MINOR_NAME=$(LIBNAME).$(DYLIBSUFFIX).$(XREDIS_MAJOR).$(XREDIS_MINOR)
DYLIB_MAJOR_NAME=$(LIBNAME).$(DYLIBSUFFIX).$(XREDIS_MAJOR)
DYLIBNAME=$(LIBNAME).$(DYLIBSUFFIX)
DYLIB_MAKE_CMD=$(CC) -shared -Wl,-soname,$(DYLIB_MINOR_NAME) -o $(DYLIBNAME) $(LDFLAGS)
STLIBNAME=$(LIBNAME).$(STLIBSUFFIX)
STLIB_MAKE_CMD=ar rcs $(STLIBNAME)

# Platform-specific overrides
uname_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')
ifeq ($(uname_S),SunOS)
  REAL_LDFLAGS+= -ldl -lnsl -lsocket
  DYLIB_MAKE_CMD=$(CC) -G -o $(DYLIBNAME) -h $(DYLIB_MINOR_NAME) $(LDFLAGS)
  INSTALL= cp -r
endif
ifeq ($(uname_S),Darwin)
  DYLIBSUFFIX=dylib
  DYLIB_MINOR_NAME=$(LIBNAME).$(XREDIS_MAJOR).$(XREDIS_MINOR).$(DYLIBSUFFIX)
  DYLIB_MAJOR_NAME=$(LIBNAME).$(XREDIS_MAJOR).$(DYLIBSUFFIX)
  DYLIB_MAKE_CMD=$(CC) -shared -Wl,-install_name,$(DYLIB_MINOR_NAME) -o $(DYLIBNAME) $(LDFLAGS)
endif

all: $(DYLIBNAME) $(STLIBNAME)

# Deps (use make dep to generate this)
xRedisClient_Connection.o: xRedisClient_connection.cpp
xRedisClient.o:            xRedisClient.cpp
xRedisClient_hashs.o:      xRedisClient_hashs.cpp
xRedisClient_keys.o:       xRedisClient_keys.cpp
xRedisClient_lists.o:      xRedisClient_lists.cpp
xRedisClient_sets.o:       xRedisClient_sets.cpp
xRedisClient_sortedsets.o: xRedisClient_sortedsets.cpp
xRedisClient_strings.o:    xRedisClient_strings.cpp
xRedisClient_pubsub.o:    xRedisClient_pubsub.cpp
xRedisPool.o:              xRedisPool.cpp

$(DYLIBNAME): $(OBJ)
	$(DYLIB_MAKE_CMD) $(OBJ)

$(STLIBNAME): $(OBJ)
	$(STLIB_MAKE_CMD) $(OBJ)

dynamic: $(DYLIBNAME)
static: $(STLIBNAME)

# Binaries:
xredis-example: examples/xredis-example.cpp $(STLIBNAME)
	$(CC) -o examples/$@ $(REAL_CFLAGS) $(REAL_LDFLAGS) -I./src -L./ $< $(STLIBNAME) -lhiredis -lpthread

examples: $(EXAMPLES)

xredis-test: test/xredis-test.cpp $(STLIBNAME)
	$(CC) -o test/$@ $(REAL_CFLAGS) $(REAL_LDFLAGS)  -I./src -L. $< $(STLIBNAME) -lhiredis -lpthread

test: xredis-test
	./test/xredis-test

%.o: %.cpp
	$(CC) $(REAL_CFLAGS) -c $< -o $@

clean:
	rm -rf $(DYLIBNAME) $(STLIBNAME) $(TESTS) src/*.o examples/example* *.o test/*.o 

dep:
	$(CC) -MM *.cpp

# Installation related variables and target
PREFIX?=/usr/local
INSTALL_INCLUDE_PATH= $(PREFIX)/include/xredis
INSTALL_LIBRARY_PATH= $(PREFIX)/lib

ifeq ($(uname_S),SunOS)
  INSTALL?= cp -r
endif

INSTALL?= cp -a

install: $(DYLIBNAME) $(STLIBNAME)
	mkdir -p $(INSTALL_INCLUDE_PATH) $(INSTALL_LIBRARY_PATH)
	$(INSTALL) src/xRedisClient.h $(INSTALL_INCLUDE_PATH)
	$(INSTALL) $(DYLIBNAME) $(INSTALL_LIBRARY_PATH)/$(DYLIB_MINOR_NAME)
	cd $(INSTALL_LIBRARY_PATH) && ln -sf $(DYLIB_MINOR_NAME) $(DYLIB_MAJOR_NAME)
	cd $(INSTALL_LIBRARY_PATH) && ln -sf $(DYLIB_MAJOR_NAME) $(DYLIBNAME)
	$(INSTALL) $(STLIBNAME) $(INSTALL_LIBRARY_PATH)

32bit:
	@echo ""
	@echo "WARNING: if this fails under Linux you probably need to install libc6-dev-i386"
	@echo ""
	$(MAKE) CFLAGS="-m32" LDFLAGS="-m32"

gprof:
	$(MAKE) CFLAGS="-pg" LDFLAGS="-pg"


.PHONY: all test check clean dep install 32bit gprof gcov noopt
