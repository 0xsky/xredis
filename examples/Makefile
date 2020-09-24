
LIBS=../src/*.o -I../src -lhiredis -lpthread
all: demo demo_cluster demo_multi_cluster xredis-example xRedisClusterClient_test

demo:
	g++ ${LIBS} demo.cpp -o demo
demo_cluster:
	g++ ${LIBS} demo_cluster.cpp -o demo_cluster
demo_multi_cluster:
	g++ ${LIBS} demo_multi_cluster.cpp -o demo_multi_cluster
xredis-example:
	g++ ${LIBS} xredis-example.cpp -o xredis-example
xRedisClusterClient_test:
	g++ ${LIBS} -I/usr/local/include/hiredis xRedisClusterClient_test.cpp -o xRedisClusterClient_test

clean:
	rm -f demo
	rm -f demo_cluster
	rm -f demo_multi_cluster
	rm -f xredis-example
	rm -f xRedisClusterClient_test

