
LIBS=../src/*.o -I../src -lhiredis -lpthread
all: demo demo_slice demo_multi_slice xredis-example xRedisClusterClient_test

demo:
	g++ ${LIBS} demo.cpp -o demo
demo_slice:
	g++ ${LIBS} demo_slice.cpp -o demo_slice
demo_multi_slice:
	g++ ${LIBS} demo_multi_slice.cpp -o demo_multi_slice
xredis-example:
	g++ ${LIBS} xredis-example.cpp -o xredis-example
xRedisClusterClient_test:
	g++ ${LIBS} -I/usr/local/include/hiredis xRedisClusterClient_test.cpp -o xRedisClusterClient_test

clean:
	rm -f demo
	rm -f demo_slice
	rm -f demo_multi_slice
	rm -f xredis-example
	rm -f xRedisClusterClient_test

