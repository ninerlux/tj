CC = g++-4.9
CFLAGS = -Wall -g -std=c++0x

tj : main.o tcp.o WorkerThread.o ConnThread.o
	${CC} ${CFLAGS} ConnThread.o WorkerThread.o tcp.o main.o -o tj

ConnThread.o : ConnThread.h ConnThread.cpp
	${CC} ${CFLAGS} -c ConnThread.cpp -o ConnThread.o

WorkerThread.o : WorkerThread.h WorkerThread.cpp
	${CC} ${CFLAGS} -c WorkerThread.cpp -o WorkerThread.o

tcp.o : tcp.h tcp.c
	${CC} ${CFLAGS} -c tcp.c -o tcp.o

main.o : main.cpp
	${CC} ${CFLAGS} -c main.cpp -o main.o

clean:
	rm -f *.o
	rm -f tj
