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

	printf("Create R: num of records = %d\n", R.num_records);

	for (i = 0; i < R.num_records; i++) {
		while ((rand = (int) random()) == 0);
      
        R.records[i].k = (join_key_t) rand % 100000;
        for (j = 0; j < BYTES_PAYLOAD_R; j++) {
            R.records[i].p.bytes[j] = ((uint8_t) random()) + 1;
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

	printf("Create S: num of records = %d\n", S.num_records);

	S.records = (struct record_s *) malloc(bytes);
	if (S.records == NULL) {
		error("malloc failed");
	}

    for (i = 0; i < S.num_records; i++) {
        while ((rand = (int) random()) == 0);
      
        S.records[i].k = (join_key_t) rand % 100000;
        for (j = 0; j < BYTES_PAYLOAD_S; j++) {
            S.records[i].p.bytes[j] = ((uint8_t) random()) + 1;
        }
    }

    return S;
}


int main(int argc, char** argv) {
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

    struct table_r R = create_table_r(atol(argv[2]) * 1024  );
    struct table_s S = create_table_s(atol(argv[2]) * 1024   * atol(argv[3]));

    if (strcmp(code, "test") == 0) {
        algo = new ProducerConsumer();
    } else if (strcmp(code, "hj") == 0) {
		algo = new HashJoin();
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
