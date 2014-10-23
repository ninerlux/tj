#include <iostream>
#include <assert.h>
#include <string.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "Algorithms.h"
#include "ConnectionLayer.h"

using namespace std;

static const char *conf = "/home/ajk2214/cs6901/tj/conf";
static const char *domain = ".clic.cs.columbia.edu";

static ConnectionLayer *CL;

void error(const char *info) {
    perror(info);
    exit(1);
}

void printListForward(ListNode *head) {
    while (head != NULL) {
        printf("%lu ", head->db.size);
        head = head->next;
    }
    printf("\n");
}

void printListBackward(ListNode *tail) {
    while (tail != NULL) {
        printf("%lu ", tail->db.size);
        tail = tail->prev;
    }
    printf("\n");
}

int main(int argc, char** argv) {
    int tags;

    ProducerConsumer *algo = new ProducerConsumer();
    tags = algo->get_tags();

    CL = new ConnectionLayer(conf, domain, tags);

    algo->run(CL);

    return 0;
}
