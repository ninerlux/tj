#include <assert.h>
#include <ctype.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <pthread.h>

#include "Algorithms.h"
#include "ConnectionLayer.h"
#include "usertype.h"

#define TAGS 2
#define MSGS 100
#define HashTableSize 100

struct worker_param {
    int tag;
    ConnectionLayer *CL;
};

static void *process_R(void *param) {
    worker_param *p = (worker_param *) param;
    int tag = p->tag;
    ConnectionLayer *CL = p->CL;

    int hosts = CL->get_hosts();
    int local_host = CL->get_local_host();
    int m;
    int n;
    DataBlock db;

    if (tag == 0) {
//        // Send something to each node, followed by a termination message
//        for (n = 0; n < hosts; n++) {
//            for (m = 0; m < MSGS; m++) {
//                while(!CL->send_begin(&db, n, tag+1));
//                sprintf((char *) db.data, "Test message %d from %d", m, local_host);
//                db.size = strlen((const char *) db.data) + 1;
//                CL->send_end(db, n, tag+1);
//            }
//
//            while(!CL->send_begin(&db, n, tag+1));
//            db.size = 0;
//            CL->send_end(db, n, tag+1);
//        }
        // Send each record in R to destination node
        int dest;
        for (i = 0; i < R.num_records; i++) {
            //hash each record's join key to get destination node number
            dest = hash(R.records[i].k);
            while (!CL->send_begin(&db, dest, 1));
            memcpy(db.data, R.records[i].p, sizeof(r_payload_t));
            db.size = sizeof(r_payload_t) + 1;      //length(db.data + 1 byte db.size)
            CL->send_end(db, dest, 1);
        }
        //send end flag to all nodes
        for (n = 0; n < hosts; n++) {
            while (!CL->send_begin(&db, n, 1));
            db.size = 0;
            CL->send_end(db, n, 1);
        }
    } else if (tag == 1) {
        // Receive until termination received from all nodes
        int src;
        int t = 0;

//        while (t != hosts) {
//            while (!CL->recv_begin(&db, &src, tag));
//
//            if (db.size > 0) {
//                printf("Node %d received \"%s\" %p from node %d\n", local_host, (char *) db.data, db.data, src);
//                fflush(stdout);
//            } else {
//                t++;
//            }
//
//            CL->recv_end(db, src, tag);
//        }
        int src_msg_num[hosts];
        while (t != hosts) {
            while (!CL->recv_begin(&db, &src, tag));

            if (db.size > 0) {
                printf("Node %d received msg %d from node %d\n", local_host, src_msg_num[src]++, src);
                fflush(stdout);
                //Add the data to hash table

            } else {
                t++;
            }

            CL->recv_end(db, src, tag);
        }
    }

    return NULL;
}

int HashJoin::hash(join_key_t k) {
    return k % HashTableSize;
}

int HashJoin::get_tags() {
    return TAGS;
}

int HashJoin::run(ConnectionLayer *CL, struct table_r R, struct table_s S) {
    int t;
    worker_threads = new pthread_t[TAGS];

    //start processing table R
    for (t = 0; t < TAGS; t++) {
        worker_param *param;
        param = new worker_param();
        param->CL = CL;
        param->tag = t;
        pthread_create(&worker_threads[t], NULL, &process_R, (void *) param);
    }
    //barrier
    for (t = 0; t < TAGS; t++) {
        void *retval;
        pthread_join(worker_threads[t], &retval);
    }

    //start processing table S
    for ()

    return 0;
}

