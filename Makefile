CC = g++
CFLAGS = -pthread -Wall -g -std=c++0x

tj : main.o tcp.o usertype.o WorkerThread.o ConnectionLayer.o
	${CC} ${CFLAGS} ConnectionLayer.o WorkerThread.o tcp.o usertype.o main.o -o tj

ConnectionLayer.o : ConnectionLayer.h ConnectionLayer.cpp
	${CC} ${CFLAGS} -c ConnectionLayer.cpp -o ConnectionLayer.o

WorkerThread.o : WorkerThread.h WorkerThread.cpp
	${CC} ${CFLAGS} -c WorkerThread.cpp -o WorkerThread.o

usertype.o: usertype.h usertype.cpp
	${CC} ${CFLAGS} -c usertype.cpp -o usertype.o

tcp.o : tcp.h tcp.c
	${CC} ${CFLAGS} -c tcp.c -o tcp.o

main.o : main.cpp
	${CC} ${CFLAGS} -c main.cpp -o main.o

clean:
	rm -f *.o
	rm -f tj
