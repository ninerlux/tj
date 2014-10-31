#include <iostream>
#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <unistd.h>

#include "Algorithms.h"
#include "ConnectionLayer.h"

using namespace std;

static const char *conf = "/home/xinlu/tj/conf";
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

struct table_r create_table_r(long bytes) {
    int i, j;
    int rand;
    struct table_r R;
    R.num_bytes = bytes;
    R.num_records = bytes / sizeof(record_r);

    R.records = (struct record_r *) malloc(bytes);
    if (R.records == NULL) {
        error("malloc failed");
    }

    for (i = 0; i < R.num_records; i++) {
        while ((rand = (int) random()) == 0);
      
        R.records[i].k = (join_key_t) rand;
        for (j = 0; j < BYTES_PAYLOAD_R; j++) {
            R.records[i].p.bytes[j] = ((uint8_t) R.records[i].k) + 1;
        }
    }

    return R;
}

struct table_s create_table_s(long bytes) {
    int i, j;
    int rand;
    struct table_s S;
    S.num_bytes = bytes;
    S.num_records = bytes / sizeof(record_s);

    S.records = (struct record_s *) malloc(bytes);
    if (S.records == NULL) {
        error("malloc failed");
    }

    for (i = 0; i < S.num_records; i++) {
        while ((rand = (int) random()) == 0);
      
        S.records[i].k = (join_key_t) rand;
        for (j = 0; j < BYTES_PAYLOAD_S; j++) {
            S.records[i].p.bytes[j] = ((uint8_t) S.records[i].k) + 1;
        }
    }

    return S;
}


int main(int argc, char** argv) {
<<<<<<< HEAD
    if (argc != 4) {
        fprintf(stderr, "Usage: join <algorithm code> <size of R in kb> <size of S as multiple of R>\n");
        return 0;
    }

    timeval t1;
    gettimeofday(&t1, NULL);
    srandom(t1.tv_usec * t1.tv_sec);

    int tags;
    char *code = argv[1];

    AbstractAlgo *algo;

    struct table_r R = create_table_r(atol(argv[2]) * 1024);
    struct table_s S = create_table_s(atol(argv[2]) * 1024 * atol(argv[3]));

    if (strcmp(code, "test") == 0) {
        algo = new ProducerConsumer();
    } else {
        fprintf(stderr, "Unrecognized algorithm code\n");
        return 0;
    }
=======
    //set up connections
    printf("Setup\n");
    fflush(stdout);
    conn = setup(conf, domain, tags, 255);
    printf("Finished setup\n");
    fflush(stdout);
    assert(conn != NULL);
    size_t h = 0;
    size_t t = 0;
    while (conn[h] != NULL) {
        for (t = 0; t < tags && conn[h][t] >= 0; t++);
        if (t == tags) {
	    h++;
	} else {
	    break;
	}
    }
    size_t local_host = h++;
    printf("local host = %lu\n", local_host);
    while (conn[h] != NULL) {
        for (t = 0; t < tags && conn[h][t] >= 0; t++);
        if (t == tags) {
	    h++;
	} else {
	    break;
	}
    }
    size_t hosts = h;
    printf("hosts = %lu\n", hosts);
    int server = conn[h + 1][0];
    printf("server = %d\n", server);
    //print out the connection matrix
    for (h = 0; h <= hosts + 1 ; h++) {
	for (t = 0; t < tags; t++) {
	    printf("%d ", conn[h][t]);
	} 
	printf("\n");
    }

    printf("Init hosts\n");
    fflush(stdout);
    init(hosts);
    printf("Finished init hosts\n");
    fflush(stdout);

    /* spawn N * T * 2 connection threads
     * N: node number
     * T: tag number
     * 2: read / write - 0: read connection, 1: write connection
     */
//    int k = 0;
//    conn_threads = new pthread_t **[hosts];
//    for (h = 0; h < hosts; h++) {
//        conn_threads[h] = new pthread_t *[tags];
//        for (t = 0; t < tags; t++) {
//            conn_threads[h][t] = new pthread_t[2];
//            thr_param *param = new thr_param();
//            param->host = h;
//            param->tag = t;
//            param->conn = conn[h][t];
//            pthread_create(&conn_threads[h][t][0], NULL, readFromSocket, (void *)param);
//            pthread_create(&conn_threads[h][t][1], NULL, writeToSocket, (void *)param);
//
//        }
//    }
//    /* spawn T worker threads
//     * T: tag number
//     */
//    worker_threads = new pthread_t[tags];
//    for (t = 0; t < tags; t++) {
//        pthread_create(&worker_threads[t], NULL, worker, NULL);
//    }
>>>>>>> d45e81dbb43e1121d6d835418da126e4160dc004

    tags = algo->get_tags();

    CL = new ConnectionLayer(conf, domain, tags);

    algo->run(CL, &R, &S);


    free(R.records);
    free(S.records);

    return 0;
}
