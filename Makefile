CC = g++
CFLAGS = -pthread -Wall -g -std=c++0x

tj : main.o tcp.o usertype.o WorkerThread.o ConnThread.o
	${CC} ${CFLAGS} ConnThread.o WorkerThread.o tcp.o usertype.o main.o -o tj

ConnThread.o : ConnThread.h ConnThread.cpp
	${CC} ${CFLAGS} -c ConnThread.cpp -o ConnThread.o

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
