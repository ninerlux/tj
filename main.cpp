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

template<typename payload_t>
payload_t key_to_payload(join_key_t k, float a) {
    payload_t p;
    uint32_t res = (uint32_t)(a * k);
    memcpy(&p, &res, sizeof(payload_t));
    return p;
}

void create_table(table_r &R, long r_bytes, table_s &S, long s_bytes) {
    int i, j;

    R.num_bytes = r_bytes;
    R.num_records = r_bytes / sizeof(record_r);

    printf("Create R: size of record_r %lu, num of records = %d\n", sizeof(record_r), R.num_records);

    R.records = (struct record_r *) malloc(r_bytes);
    if (R.records == NULL) {
        error("malloc R failed");
    }

    for (i = 0; i < R.num_records; i++) {
        join_key_t rand;
        while ((rand = (uint32_t) random()) == 0);
        R.records[i].k = rand;
        for (int b = 0; b < BYTES_PAYLOAD_R; b++) {
            R.records[i].p.bytes[b] = ((uint8_t) R.records[i].k) + 1;
        }
    }

    S.num_bytes = s_bytes;
    S.num_records = s_bytes / sizeof(record_s);

    printf("Create S: num of records = %d\n", S.num_records);

    S.records = (struct record_s *) malloc(s_bytes);
    if (S.records == NULL) {
        error("malloc S failed");
    }

    for (j = 0; j < S.num_records; j++) {
        join_key_t rand;
        while ((rand = (uint32_t) random()) == 0);
        S.records[j].k = rand;
        for (int b = 0; b < BYTES_PAYLOAD_S; b++) {
            S.records[j].p.bytes[b] = ((uint8_t) S.records[j].k) + 1;
        }
    }
}


int main(int argc, char **argv) {
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

    struct table_r R;
    struct table_s S;
    create_table(R, atol(argv[2]) * 32, S, atol(argv[2]) * 32 * atol(argv[3]));

    if (strcmp(code, "test") == 0) {
        algo = new ProducerConsumer();
    } else if (strcmp(code, "hj") == 0) {
        algo = new HashJoin();
    } else if (strcmp(code, "tj2") == 0) {
        algo = new TrackJoin2();
    } else {
        fprintf(stderr, "Unrecognized algorithm code\n");
        return 0;
    }

    tags = algo->get_tags();

    CL = new ConnectionLayer(conf, domain, tags);

    algo->run(CL, &R, &S);


    free(R.records);
    free(S.records);

    return 0;
}
