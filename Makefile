CC = g++
CFLAGS = -pthread -Wall -g -std=c++0x

join : main.o tcp.o usertype.o TrackJoin4.o TrackJoin2.o HashJoin.o ProducerConsumer.o ConnectionLayer.o
	${CC} ${CFLAGS} ConnectionLayer.o ProducerConsumer.o HashJoin.o TrackJoin4.o TrackJoin2.o tcp.o usertype.o main.o -o join

ConnectionLayer.o : ConnectionLayer.h ConnectionLayer.cpp
	${CC} ${CFLAGS} -c ConnectionLayer.cpp -o ConnectionLayer.o

TrackJoin4.o : Algorithms.h HashTable.h TrackJoin2.cpp
	${CC} ${CFLAGS} -c TrackJoin4.cpp -o TrackJoin4.o

TrackJoin2.o : Algorithms.h HashTable.h TrackJoin2.cpp
	${CC} ${CFLAGS} -c TrackJoin2.cpp -o TrackJoin2.o

HashJoin.o : Algorithms.h HashTable.h HashJoin.cpp
	${CC} ${CFLAGS} -c HashJoin.cpp -o HashJoin.o

ProducerConsumer.o : Algorithms.h ProducerConsumer.cpp
	${CC} ${CFLAGS} -c ProducerConsumer.cpp -o ProducerConsumer.o

usertype.o: usertype.h usertype.cpp
	${CC} ${CFLAGS} -c usertype.cpp -o usertype.o

tcp.o : tcp.h tcp.c
	${CC} ${CFLAGS} -c tcp.c -o tcp.o

main.o : main.cpp
	${CC} ${CFLAGS} -c main.cpp -o main.o

clean:
	rm -f *.o
	rm -f join
