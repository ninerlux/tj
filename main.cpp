#include <iostream>
#include <assert.h>
#include <string.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "ConnectionLayer.h"

using namespace std;

const char *conf = "/home/ajk2214/cs6901/tj/conf";
const char *domain = ".clic.cs.columbia.edu";
const int tags = 2;
const int conn_type = 2; // Two types of connection. 0: read; 1: write.

ConnectionLayer *CL;

pthread_t *worker_threads;

void error(const char *info) {
    perror(info);
    exit(1);
}


void *worker(void *param) {
    thr_param *p = (thr_param *) param;
    int tag = p->tag;
    int hosts = CL->get_hosts();
    int local_host = CL->get_local_host();
    int m;
    int n;
    DataBlock db;

    if (tag == 0) {
        // Send something to each node, followed by a termination message
        for (n = 0; n < hosts; n++) {
            for (m = 0; m < 100; m++) {
                while(!CL->send_begin(&db, n, tag+1));
                sprintf((char *) db.data, "Test message %d from %d", m, local_host);
                db.size = strlen((const char *) db.data) + 1;
                CL->send_end(db, n, tag+1);
            }

            while(!CL->send_begin(&db, n, tag+1));
            db.size = 0;
            CL->send_end(db, n, tag+1);
        }
    } else if (tag == 1) {
        // Receive until termination received from all nodes
        DataBlock db;
        int src;
        int t = 0;

        while (t != hosts) {
            while (!CL->recv_begin(&db, &src, tag));

            if (db.size > 0) {
                printf("Node %d received \"%s\" %p from node %d\n", local_host, (char *) db.data, db.data, src);
                fflush(stdout);
            } else {
                t++;
            }

            CL->recv_end(db, src, tag);
        }
    }

    return NULL;
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
    CL = new ConnectionLayer(conf, domain, tags);

    /* spawn T worker threads
     * T: tag number
     */
    int t;
    worker_threads = new pthread_t[tags];
    for (t = 0; t < tags; t++) {
        thr_param *param;
        param = new thr_param();
        param->tag = t;
        pthread_create(&worker_threads[t], NULL, &worker, (void *) param);
    }

    for (t = 0; t < tags; t++) {
        void *retval;
        pthread_join(worker_threads[t], &retval);
    }

    return 0;
}
