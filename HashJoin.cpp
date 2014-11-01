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
#define HASH_TABLE_SIZE 1000

struct worker_param {
    int tag;
    ConnectionLayer *CL;
    table_r *R;
    table_s *S;
    HashTable *h;
};

static void *process_R(void *param) {
    worker_param *p = (worker_param *) param;
    int tag = p->tag;
    ConnectionLayer *CL = p->CL;
    table_r *R = p->R;
    HashTable *h_table = p->h;

    int hosts = CL->get_hosts();
    int local_host = CL->get_local_host();
    int i;
    int n;
    DataBlock db;

    if (tag == 0) {
        // Send each record in R to destination node
        int dest;
        for (i = 0; i < R->num_records; i++) {
            // hash each record's join key to get destination node number
            // hash() is the hash function of hash table. It is like "key % p", where p is a very large prime
            dest = h_table->hash(R->records[i].k) % hosts;
            while (!CL->send_begin(&db, dest, 1));
            memcpy(db.data, &R->records[i].k, sizeof(join_key_t));
            memcpy((char *)db.data + sizeof(join_key_t), &R->records[i].p, sizeof(r_payload_t));
			db.size = sizeof(join_key_t) + sizeof(r_payload_t);
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
        record_r *r;
        int t = 0;
        int *src_msg_num = new int[hosts];

		for (int h = 0; h < hosts; h++) {
			src_msg_num[h] = 0;
		}

        while (t != hosts) {
            while (!CL->recv_begin(&db, &src, tag));

            if (db.size > 0) {
                assert(db.size == sizeof(join_key_t) + sizeof(r_payload_t));
                r = new record_r();
                memcpy(&r->k, db.data, sizeof(join_key_t));
                memcpy(&r->p, (char *)db.data + sizeof(join_key_t), sizeof(r_payload_t));
                //printf("R - Node %d received record_r (%u, %u) from node %d with size %lu\n", local_host, r->k, r->p, src, db.size);
                //fflush(stdout);
                //Add the data to hash table
				int ret;
                if ((ret = h_table->add(r)) < 0) {
                    printf("Error: R - add data to hash table failed\n");
                    fflush(stdout);
                    pthread_exit(NULL);
				}
            } else {
                t++;
            }

            CL->recv_end(db, src, tag);
        }
    }

    return NULL;
}

static void *process_S(void *param) {
    worker_param *p = (worker_param *) param;
    int tag = p->tag;
    ConnectionLayer *CL = p->CL;
    table_s *S = p->S;
    HashTable *h_table = p->h;

    int hosts = CL->get_hosts();
    int local_host = CL->get_local_host();
    int i;
    int n;
    DataBlock db;

    if (tag == 0) {
        // Send each record in R to destination node
        int dest;
        for (i = 0; i < S->num_records; i++) {
            // hash each record's join key to get destination node number
            // hash() is the hash function of hash table. It is like "key % p", where p is a very large prime
            dest = h_table->hash(S->records[i].k) % hosts;
            while (!CL->send_begin(&db, dest, 1));
            memcpy(db.data, &S->records[i].k, sizeof(join_key_t));
            memcpy((char *)db.data + sizeof(join_key_t), &S->records[i].p, sizeof(s_payload_t));
            db.size = sizeof(join_key_t) + sizeof(s_payload_t);
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
        record_s *s;
        record_r *r = NULL;
        int t = 0;
        int *src_msg_num = new int[hosts];

		for (int h = 0; h < hosts; h++) {
			src_msg_num[h] = 0;
		}

		int join_num = 0;
        while (t != hosts) {
            while (!CL->recv_begin(&db, &src, tag));

            if (db.size > 0) {
                assert(db.size == sizeof(join_key_t) + sizeof(s_payload_t));
                s = new record_s();
                memcpy(&s->k, db.data, sizeof(join_key_t));
                memcpy(&s->p, (char *)db.data + sizeof(join_key_t), sizeof(s_payload_t));
                //printf("S - Node %d received record_s (%u, %u) from node %d with size %lu\n", local_host, s->k, s->p, src, db.size);
                //fflush(stdout);
                //Probe data in hash table
				int ret = - 2;		//set 1st time starting searching index (ret + 1) as -1. 
				//if ((ret = h_table->find(s->k, &r)) >= 0) {
				//	//Output joined tuples
				//	printf("Join Result: Node %d #%d, join_key %u payload_r %u, payload_s %u\n", local_host, join_num++, s->k, r->p, s->p);
				//	fflush(stdout);
				//}	
				while ((ret = h_table->find(s->k, ret + 1, &r)) >= 0) {
					//Output joined tuples
					printf("Join Result: Node %d #%d, join_key %u payload_r %u, payload_s %u\n", local_host, ++join_num, s->k, r->p, s->p);
					fflush(stdout);
				}
			} else {
                t++;
            }

            CL->recv_end(db, src, tag);
        }
    }

    return NULL;
}

int HashJoin::get_tags() {
    return TAGS;
}

int HashJoin::run(ConnectionLayer *CL, table_r *R, table_s *S) {
    int t;
    worker_threads = new pthread_t[TAGS];

    //create HashTable h_table
    HashTable *h_table = new HashTable(HASH_TABLE_SIZE);

	
    //start processing table R
    for (t = 0; t < TAGS; t++) {
        worker_param *param;
        param = new worker_param();
        param->CL = CL;
        param->tag = t;
        param->R = R;
        param->S = NULL;
        param->h = h_table;
        pthread_create(&worker_threads[t], NULL, &process_R, (void *) param);
    }
    //barrier
    for (t = 0; t < TAGS; t++) {
        void *retval;
        pthread_join(worker_threads[t], &retval);
    }

    //start processing table S
    for (t = 0; t < TAGS; t++) {
        worker_param *param;
        param = new worker_param();
        param->CL = CL;
        param->tag = t;
        param->R = NULL;
        param->S = S;
        param->h = h_table;
        pthread_create(&worker_threads[t], NULL, &process_S, (void *) param);
    }
    //wait threads to finish
    for (t = 0; t < TAGS; t++) {
        void *retval;
        pthread_join(worker_threads[t], &retval);
    }

    return 0;
}

