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

template <typename Table>
class worker_param {
public:
    int tag;
    ConnectionLayer *CL;
    Table *t;
    int scan_start_index, scan_end_index;
    HashTable *h;
};

template <typename Table, typename Record>
static void *scan_and_send(void *param) {
    worker_param<Table> *p = (worker_param<Table> *) param;
    ConnectionLayer *CL = p->CL;
    Table *T = p->t;
    HashTable *h_table = p->h;

    int hosts = CL->get_hosts();
    int local_host = CL->get_local_host();

    DataBlock *dbs = new DataBlock[hosts];
    // prepare data blocks for each destination
    int dest;
    for (dest = 0; dest < hosts; dest++) {
        while (!CL->send_begin(&dbs[dest], dest, 1));
    }
    // Send each record in T to destination node
    for (int i = 0; i < T->num_records; i++) {
        // hash each record's join key to get destination node number
        // hash() is the hash function of hash table. It is like "key % p", where p is a very large prime
        dest = h_table->hash32(T->records[i].k) % hosts;
        if (dbs[dest].size + sizeof(Record) > BLOCK_SIZE) {
            CL->send_end(dbs[dest], dest, 1);
            printf("Scan - Node %d send data block to node %d with size %lu\n", local_host, dest, dbs[dest].size);
            fflush(stdout);
            while (!CL->send_begin(&dbs[dest], dest, 1));
        }
        *((Record *) dbs[dest].data + dbs[dest].size / sizeof(Record)) = T->records[i];
        dbs[dest].size += sizeof(Record);
    }
    // Send last partially filled data blocks and end flags to all nodes
    for (dest = 0; dest < hosts; dest++) {
        // Send last data blocks
        if (dbs[dest].size > 0) {
            CL->send_end(dbs[dest], dest, 1);
            printf("Scan - Node %d send data block to node %d with size %lu\n", local_host, dest, dbs[dest].size);
            fflush(stdout);
        }
        // Send end flags
        while (!CL->send_begin(&dbs[dest], dest, 1));
        dbs[dest].size = 0;
        CL->send_end(dbs[dest], dest, 1);
        printf("Scan - Node %d send end flag node %d with size %lu\n", local_host, dest, dbs[dest].size);
        fflush(stdout);
    }

    return NULL;
}

static void *receive_and_build(void *param) {
    worker_param<table_r> *p = (worker_param<table_r> *) param;
    ConnectionLayer *CL = p->CL;
    HashTable *h_table = p->h;

    int hosts = CL->get_hosts();
    int local_host = CL->get_local_host();

    int src;
    record_r *r;
    DataBlock db;

    int t = 0;
    // Receive until termination received from all nodes
    while (t != hosts) {
        while (!CL->recv_begin(&db, &src, 1));
        printf("R - Node %d received data block from node %d with size %lu\n", local_host, src, db.size);
        fflush(stdout);
        if (db.size > 0) {
            int records_copied = 0;
            while (records_copied * sizeof(record_r) < db.size) {
                r = new record_r();
                assert((records_copied + 1) * sizeof(record_r) <= db.size);
                *r = *((record_r *) db.data + records_copied);
                records_copied += 1;
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
        CL->recv_end(db, src, 1);
    }

    return NULL;
}

template <typename payload_t>
join_key_t payload_to_key(payload_t p, float b) {
	uint32_t payload;
	memcpy(&payload, &p, sizeof(payload_t));
	join_key_t k = (join_key_t) (b * payload);
	return k;
}

static void *receive_and_probe(void *param) {
    worker_param<table_s> *p = (worker_param<table_s> *) param;
    ConnectionLayer *CL = p->CL;
    HashTable *h_table = p->h;

    int hosts = CL->get_hosts();
    int local_host = CL->get_local_host();

    int src;
    record_s *s;
    record_r *r = NULL;
    DataBlock db;

    int t = 0;
    int join_num = 0;
    // Receive until termination received from all nodes
    while (t != hosts) {
        while (!CL->recv_begin(&db, &src, 1));
        printf("S - Node %d received data block from node %d with size %lu\n", local_host, src, db.size);
        fflush(stdout);
        if (db.size > 0) {
            size_t records_copied = 0;
            while (records_copied * sizeof(record_s) < db.size) {
                s = new record_s();
                assert((records_copied + 1) * sizeof(record_s) <= db.size);
                *s = *((record_s *) db.data + records_copied);
                records_copied += 1;
                //Probe data in hash table
                int ret = h_table->getNum() - 1;        //set 1st time starting searching index (ret + 1) as table size.
                while ((ret = h_table->find(s->k, &r, ret + 1, 10)) >= 0) {
					//Validate key-value mapping for r and s
					bool valid = false;
					if (s->k == r->k && payload_to_key<r_payload_t>(r->p, 1 / 131) == payload_to_key<s_payload_t>(s->p, 1 / 181)) {
						valid = true;
					}
                    //Output joined tuples
                    printf("Join Result: Node %d #%d, join_key %u payload_r %u, payload_s %u %s\n", local_host, ++join_num,
                            s->k, r->p, s->p, valid ? "correct" : "incorrect");
                    fflush(stdout);
                }
            }
        } else {
            t++;
        }
        CL->recv_end(db, src, 1);
    }

    return NULL;
}

int HashJoin::get_tags() {
    return TAGS;
}

int HashJoin::run(ConnectionLayer *CL, table_r *R, table_s *S) {
    int t;
    worker_threads = new pthread_t[32];

    //create HashTable h_table
    HashTable *h_table = new HashTable(HASH_TABLE_SIZE);

    //start table R scan_and_send
    worker_param<table_r> *param_r = new worker_param<table_r>();
    param_r->CL = CL;
    param_r->t = R;
    param_r->h = h_table;
    pthread_create(&worker_threads[0], NULL, &scan_and_send<table_r, record_r>, (void *) param_r);

    //start table R receive_and_build
    pthread_create(&worker_threads[1], NULL, &receive_and_build, (void *) param_r);

    //barrier
    for (t = 0; t < 2; t++) {
        void *retval;
        pthread_join(worker_threads[t], &retval);
    }

    //start table S scan_and_send
    worker_param<table_s> *param_s = new worker_param<table_s>();
    param_s->CL = CL;
    param_s->t = S;
    param_s->h = h_table;
    pthread_create(&worker_threads[0], NULL, &scan_and_send<table_s, record_s>, (void *) param_s);

    //start table S receive_and_build
    pthread_create(&worker_threads[1], NULL, &receive_and_probe, (void *) param_s);

    //wait threads to finish
    for (t = 0; t < 2; t++) {
        void *retval;
        pthread_join(worker_threads[t], &retval);
    }

    return 0;
}

