CC = g++
CFLAGS = -Wall -g -std=c++0x

tj : main.o tcp.o
	${CC} ${CFLAGS} tcp.o main.o -o tj

tcp.o : tcp.c
	${CC} ${CFLAGS} -c tcp.c -o tcp.o

main.o : main.cpp
	${CC} ${CFLAGS} -c main.cpp -o main.o

clean:
	rm -f *.o
	rm -f tj
