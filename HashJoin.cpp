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

	if (tag == 0) {
		DataBlock *dbs = new DataBlock[hosts];
		// prepare data blocks for each destination
		int dest;
		for (dest = 0; dest < hosts; dest++) {
			while (!CL->send_begin(&dbs[dest], dest, 1));
		}
        // Send each record in R to destination node
        for (int i = 0; i < R->num_records; i++) {
            // hash each record's join key to get destination node number
            // hash() is the hash function of hash table. It is like "key % p", where p is a very large prime
            dest = h_table->hash(R->records[i].k) % hosts;
            if (dbs[dest].size + sizeof(record_r) > BLOCK_SIZE) {
				CL->send_end(dbs[dest], dest, 1);
				printf("R1 - Node %d send data block to node %d with size %lu\n", local_host, dest, dbs[dest].size);
				fflush(stdout); 
				while (!CL->send_begin(&dbs[dest], dest, 1));
			}
            *((record_r *)dbs[dest].data + dbs[dest].size / sizeof(record_r)) = R->records[i];
            dbs[dest].size += sizeof(record_r);
		}
        // Send last partially filled data blocks and end flags to all nodes
        for (dest = 0; dest < hosts; dest++) {
			// Send last data blocks
			if (dbs[dest].size > 0) {
				CL->send_end(dbs[dest], dest, 1);
				printf("R2 - Node %d send data block to node %d with size %lu\n", local_host, dest, dbs[dest].size);
				fflush(stdout); 
			}		
			// Send end flags
            while (!CL->send_begin(&dbs[dest], dest, 1));
            dbs[dest].size = 0;
			CL->send_end(dbs[dest], dest, 1);
			printf("R3 - Node %d send data block to node %d with size %lu\n", local_host, dest, dbs[dest].size);
			fflush(stdout); 
		}		
    } else if (tag == 1) {
        int src;
        record_r *r;
		DataBlock db;

        int t = 0;
        // Receive until termination received from all nodes
        while (t != hosts) {
            while (!CL->recv_begin(&db, &src, tag));
			printf("R - Node %d received data block from node %d with size %lu\n", local_host, src, db.size);
			fflush(stdout);
            if (db.size > 0) {
				int records_copied = 0;
				while (records_copied * sizeof(record_r) < db.size) { 
					r = new record_r();
					assert((records_copied + 1) * sizeof(record_r) <= db.size);
                    *r = *((record_r *)db.data + records_copied);
					records_copied += 1;
					//printf("R - Node %d received record_r (%u, %u) from node %d\n", local_host, r->k, r->p, src);
					//fflush(stdout);
					//Add the data to hash table
					int ret;
					if ((ret = h_table->add(r)) < 0) {
						printf("Error: R - add data to hash table failed\n");
						fflush(stdout);
						pthread_exit(NULL);
					}
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

    if (tag == 0) {
        DataBlock *dbs = new DataBlock[hosts];
		// prepare data blocks for each destination
		int dest;
		for (dest = 0; dest < hosts; dest++) {
			while (!CL->send_begin(&dbs[dest], dest, 1));
		}
		// Send each record in S to destination node
        for (int i = 0; i < S->num_records; i++) {
            // hash each record's join key to get destination node number
            // hash() is the hash function of hash table. It is like "key % p", where p is a very large prime
			dest = h_table->hash(S->records[i].k) % hosts;
            if (dbs[dest].size + sizeof(record_s) > BLOCK_SIZE) {
				CL->send_end(dbs[dest], dest, 1);
				printf("S1 - Node %d send data block to node %d with size %lu\n", local_host, dest, dbs[dest].size);
				fflush(stdout); 
				while (!CL->send_begin(&dbs[dest], dest, 1));
			}
			*((record_s *)dbs[dest].data + dbs[dest].size / sizeof(record_s)) = S->records[i];
			dbs[dest].size += sizeof(record_s);
		}
		// Send last partially filled data blocks and end flags to all nodes
		for (dest = 0; dest < hosts; dest++) {
			// Send last data blocks
			if (dbs[dest].size > 0) {
				CL->send_end(dbs[dest], dest, 1);
				printf("S2 - Node %d send data block to node %d with size %lu\n", local_host, dest, dbs[dest].size);
				fflush(stdout);
			}
			// Send end flags
			while (!CL->send_begin(&dbs[dest], dest, 1));
			dbs[dest].size = 0;
			CL->send_end(dbs[dest], dest, 1);
			printf("S3 - Node %d send data block to node %d with size %lu\n", local_host, dest, dbs[dest].size);
			fflush(stdout);
		}    
	} else if (tag == 1) {
        int src;
        record_s *s;
        record_r *r = NULL;
		DataBlock db;

        int t = 0;
		int join_num = 0;
        // Receive until termination received from all nodes
        while (t != hosts) {
            while (!CL->recv_begin(&db, &src, tag));
			printf("S - Node %d received data block from node %d with size %lu\n", local_host, src, db.size);
			fflush(stdout);
            if (db.size > 0) {
				size_t records_copied = 0;
				while (records_copied * sizeof(record_s) < db.size) {
					s = new record_s();
					assert((records_copied + 1) * sizeof(record_s) <= db.size);	
					*s = *((record_s *)db.data + records_copied);
					records_copied += 1;
					//printf("S - Node %d received record_s (%u, %u) from node %d with size %lu\n", local_host, s->k, s->p, src, db.size);
					//fflush(stdout);
					//Probe data in hash table
					int ret = - 2;		//set 1st time starting searching index (ret + 1) as -1. 
					while ((ret = h_table->find(s->k, ret + 1, &r)) >= 0) {
						//Output joined tuples
						printf("Join Result: Node %d #%d, join_key %u payload_r %u, payload_s %u\n", local_host, ++join_num, 
								s->k, r->p, s->p);
						fflush(stdout);
					}
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

