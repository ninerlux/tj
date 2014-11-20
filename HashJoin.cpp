#include <assert.h>
#include <ctype.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <pthread.h>

#include "Algorithms.h"
#include "ConnectionLayer.h"
#include "usertype.h"

#define TAGS 2
#define CPU_CORES 16
#define R_SEND_THREADS 4
#define R_RECV_THREADS 12
#define S_SEND_THREADS 4
#define S_RECV_THREADS 12

int nodes_recv_complete;
pthread_mutex_t nrc_mutex;
	
size_t added_tuples = 0;
size_t queried_num = 0;
size_t join_num = 0;

int get_nrc() {
	pthread_mutex_lock(&nrc_mutex);
	int r = nodes_recv_complete;
	pthread_mutex_unlock(&nrc_mutex);
	return r;
}

void add_nrc(int n = 1) {
	pthread_mutex_lock(&nrc_mutex);
	nodes_recv_complete += n;
	pthread_mutex_unlock(&nrc_mutex);
}

template <typename Table>
class worker_param {
public:
    int tag;
    ConnectionLayer *CL;
    Table *t;
    int start_index, end_index;
    HashTable *h;
};

template <typename Table, typename Record>
static void *scan_and_send(void *param) {
    worker_param<Table> *p = (worker_param<Table> *) param;
    ConnectionLayer *CL = p->CL;
    Table *T = p->t;
	int tag = p->tag;
	size_t start = p->start_index;
	size_t end = p->end_index;
    HashTable *h_table = p->h;

    int hosts = CL->get_hosts();
    //int local_host = CL->get_local_host();

    DataBlock *dbs = new DataBlock[hosts];
    // prepare data blocks for each destination
    int dest;
    for (dest = 0; dest < hosts; dest++) {
        while (!CL->send_begin(&dbs[dest], dest, tag));
        dbs[dest].size = 0;
    }

	//printf("Node %d : tag %d, start %lu, end %lu\n", local_host, tag, start, end);

    // Send each record in T to destination node
    for (size_t i = start; i < end; i++) {
        // hash each record's join key to get destination node number
        // hash() is the hash function of hash table. It is like "key % p", where p is a very large prime
        dest = h_table->hash32(T->records[i].k) % hosts;
        if (dbs[dest].size + sizeof(Record) > BLOCK_SIZE) {
            CL->send_end(dbs[dest], dest, tag);
            //printf("Scan - Node %d send data block to node %d with %lu records, tag %d\n", local_host, dest, dbs[dest].size / sizeof(Record), tag);
            //fflush(stdout);
            while (!CL->send_begin(&dbs[dest], dest, tag));
            dbs[dest].size = 0;
        }
        *((Record *) dbs[dest].data + dbs[dest].size / sizeof(Record)) = T->records[i];
        dbs[dest].size += sizeof(Record);
        assert(dbs[dest].size <= BLOCK_SIZE);
    }
    // Send last partially filled data blocks and end flags to all nodes
    for (dest = 0; dest < hosts; dest++) {
        // Send last data blocks
        if (dbs[dest].size > 0) {
            assert(dbs[dest].size <= BLOCK_SIZE);
            CL->send_end(dbs[dest], dest, tag);
            //printf("Scan - Node %d send data block to node %d with %lu records,tag %d\n", local_host, dest, dbs[dest].size / sizeof(Record), tag);
            //fflush(stdout);
        }
    }
	//printf("Node %d - scan and send: I RETURN !!!\n", local_host);
    //fflush(stdout);


    return NULL;
}

static void *receive_and_build(void *param) {
    worker_param<table_r> *p = (worker_param<table_r> *) param;
    ConnectionLayer *CL = p->CL;
	int tag = p->tag;
    HashTable *h_table = p->h;

    int hosts = CL->get_hosts();
    //int local_host = CL->get_local_host();

    int src;
    record_r *r;
    DataBlock db;

    // Receive until termination received from all nodes
    while (nodes_recv_complete < hosts) {
		int ret;
		while ((ret = CL->recv_begin(&db, &src, tag)) == 0 && nodes_recv_complete < hosts);
		if (ret != 0) {
			//printf("R - Node %d received data block from node %d with %lu records\n", local_host, src, db.size / sizeof(record_r));
			//fflush(stdout);
			assert(db.size <= BLOCK_SIZE);
			if (db.size > 0) {
				size_t bytes_copied = 0;
				while (bytes_copied < db.size) {
					r = new record_r();
					assert(bytes_copied + sizeof(record_r) <= db.size);
					*r = *((record_r *)db.data + bytes_copied / sizeof(record_r));
					bytes_copied += sizeof(record_r);
					//Add the data to hash table
					if ((ret = h_table->add(r)) < 0) {
						printf("HashTable full!!! added items = %lu\n", added_tuples);
						fflush(stdout);
						assert(ret >= 0);
					} else {
						added_tuples++;
					}
				}
			} else {
				add_nrc();
				//printf("R - Node %d recv end flag, nrc = %d\n", local_host, get_nrc());
				//fflush(stdout);
			}
			CL->recv_end(db, src, tag);
		}
    }
	
	//printf("Node %d - receive and build: I RETURN !!!\n", local_host);
	//fflush(stdout);

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
	int tag = p->tag;

    int hosts = CL->get_hosts();
    //int local_host = CL->get_local_host();

    int src;
    record_s *s;
    record_r *r = NULL;
    DataBlock db;

    // Receive until termination received from all nodes
    while (nodes_recv_complete < hosts) {
		int ret;
		while ((ret = CL->recv_begin(&db, &src, tag)) == 0 && nodes_recv_complete < hosts) {
			//printf("Node %d BLOCKING HERE!! nrc %d\n", local_host, get_nrc());
			//fflush(stdout);
		}
		//printf("Node %d I AM HERE 111  !! nrc %d\n", local_host, nodes_recv_complete);
		if (ret != 0) {
			//printf("S - Node %d received data block from node %d with %lu records\n", local_host, src, db.size / sizeof(record_s));
			//fflush(stdout);
			assert(db.size <= BLOCK_SIZE);
			if (db.size > 0) {
				size_t bytes_copied = 0;
				while (bytes_copied < db.size) {
					s = new record_s();
					assert(bytes_copied + sizeof(record_s) <= db.size);
					*s = *((record_s *)db.data + bytes_copied / sizeof(record_s));
					bytes_copied += sizeof(record_s);
					//Probe data in hash table
					ret = h_table->getNum();        //set 1st time starting searching index (ret + 1) as table size + 1.
					//printf("Node %d I AM HERE 222 !! nrc %d\n", local_host, nodes_recv_complete);
					queried_num++;
					while ((ret = h_table->find(s->k, &r, ret + 1, 10)) >= 0) {
						//printf("Node %d I AM HERE 333  !! nrc %d join_num %lu key %u ret %d\n", local_host, nodes_recv_complete, join_num, s->k, ret);
		
						//Validate key-value mapping for r and s
						bool valid = false;
						if (s->k == r->k && payload_to_key<r_payload_t>(r->p, 1 / 131) == payload_to_key<s_payload_t>(s->p, 1 / 181)) {
							valid = true;
						}
						assert(valid == true);
						//Output joined tuples
						//printf("Join Result: Node %d #%d, join_key %u payload_r %u, payload_s %u %s\n", local_host, ++join_num,
						//        s->k, r->p, s->p, valid ? "correct" : "incorrect");
						//fflush(stdout);
						join_num++;
					}
				}
			} else {
				add_nrc();
			}
			CL->recv_end(db, src, tag);
		}
	}

	//printf("Node %d - receive and probe: I RETURN!!!\n", local_host);
	//fflush(stdout);

	return NULL;
}

int HashJoin::get_tags() {
    return TAGS;
}

int HashJoin::run(ConnectionLayer *CL, table_r *R, table_s *S) {
    int t;
	int local_host = CL->get_local_host();
    worker_threads = new pthread_t[16];

    //create HashTable h_table
	size_t h_table_size = R->num_records / 0.5;
	printf("hash table size = %lu\n", h_table_size);
	fflush(stdout);
    HashTable *h_table = new HashTable(h_table_size);

	pthread_mutex_init(&nrc_mutex, NULL);

	int times, timed;

	times = time(NULL);

	// Allocate #R_SEND_THREADS threads to scan_and_send R table
	size_t interval = R->num_records / R_SEND_THREADS;
	for (t = 0; t < R_SEND_THREADS; t++) {
		worker_param<table_r> *param_r = new worker_param<table_r>();
		param_r->CL = CL;
		param_r->t = R;
		param_r->tag = 0;
		param_r->h = h_table;
		param_r->start_index = interval * t;
		param_r->end_index = interval * (t + 1);
		pthread_create(&worker_threads[t], NULL, &scan_and_send<table_r, record_r>, (void *) param_r);
	}

	// Allocate #R_RECV_THREADS threads to recv_and_build R tuples
	nodes_recv_complete = 0;

	for (t = R_SEND_THREADS; t < R_SEND_THREADS + R_RECV_THREADS; t++) {
        worker_param<table_r> *param_r = new worker_param<table_r>();
		param_r->CL = CL;
		param_r->t = R;
		param_r->tag = 0;
		param_r->h = h_table;
		pthread_create(&worker_threads[t], NULL, &receive_and_build, (void *) param_r);
    }

    // barrier for threads sending R tuples
    for (t = 0; t < R_SEND_THREADS; t++) {
        void *retval;
        pthread_join(worker_threads[t], &retval);
    }

	// send end flag to each node
	int hosts = CL->get_hosts();
	DataBlock *dbs = new DataBlock[hosts];
	for (int dest = 0; dest < hosts; dest++) {
		while (!CL->send_begin(&dbs[dest], dest, 0));  //tag for R is 0
		dbs[dest].size = 0;
		CL->send_end(dbs[dest], dest, 0);
		//printf("Sync - Node %d send R end flag to node %d with size %lu, tag %d\n", local_host, dest, dbs[dest].size, 0);
		//fflush(stdout);
	}

	// barrier for threads receiving R tuples and building local host table
	for (t = R_SEND_THREADS; t < CPU_CORES; t++) {
		void *retval;
		pthread_join(worker_threads[t], &retval);
	}

	printf("Node %d add items %lu\n", local_host, added_tuples);
	fflush(stdout);

	// Allocate #S_SEND_THREADS threads to scan_and_send S table
	interval = S->num_records / S_SEND_THREADS;
	for (t = 0; t < S_SEND_THREADS; t++) {
		worker_param<table_s> *param_s = new worker_param<table_s>();
		param_s->CL = CL;
		param_s->t = S;
		param_s->tag = 1;
		param_s->h = h_table;
		param_s->start_index = interval * t;
		param_s->end_index = interval * (t + 1);
		pthread_create(&worker_threads[t], NULL, &scan_and_send<table_s, record_s>, (void *) param_s);
	}

	// barrier for threads sending S tuples
	for (t = 0; t < S_SEND_THREADS; t++) {
		void *retval;
		pthread_join(worker_threads[t], &retval);
	}

	// send end flag to each node
	//*dbs = new DataBlock[hosts];
	for (int dest = 0; dest < hosts; dest++) {
		while (!CL->send_begin(&dbs[dest], dest, 1));  //tag for S is 1
		dbs[dest].size = 0;
		CL->send_end(dbs[dest], dest, 1);
		//printf("Sync - Node %d send S end flag to node %d with size %lu, tag %d\n", local_host, dest, dbs[dest].size, 0);
		//fflush(stdout);
	}

	// Allocate #S_RECV_THREADS threads to recv_and_probe S tuples
	nodes_recv_complete = 0;

	for (t = S_SEND_THREADS; t < S_SEND_THREADS + S_RECV_THREADS; t++) {
		worker_param<table_s> *param_s = new worker_param<table_s>();
		param_s->CL = CL;
		param_s->t = S;
		param_s->tag = 1;
		param_s->h = h_table;
		pthread_create(&worker_threads[t], NULL, &receive_and_probe, (void *) param_s);
	}

    // barrier for threads receiving S tuples and probing local hash table
    for (t = S_SEND_THREADS; t < CPU_CORES; t++) {
        void *retval;
        pthread_join(worker_threads[t], &retval);
    }

	printf("Node %d JOIN NUM = %lu, queried num = %lu\n", local_host, join_num, queried_num);
    fflush(stdout);

	timed = time(NULL);
	printf("Total time taken: %ds\n", timed - times);

    return 0;
}

