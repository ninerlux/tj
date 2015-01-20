#include "Algorithms.h"
#include "ConnectionLayer.h"
#include "usertype.h"
#include "HashTable.h"
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <unistd.h>

#define CPU_CORES 32
#define R_SCAN_THREADS 8
#define S_SCAN_THREADS 8
#define RECV_R_THREADS 8
#define RECV_S_THREADS 8
#define T_NOTI_THREADS 12
#define R_SEND_THREADS 10
#define S_COMT_THREADS 10

int *recv_complete_tj2 = new int[4];
pthread_mutex_t *rc_mutex_tj2 = new pthread_mutex_t[4];

size_t trackjoin2_num = 0;
pthread_mutex_t tj_num_mutex;

size_t added_r_num = 0;
size_t added_s_num = 0;

size_t recv_tuples = 0;

int get_rc_tj2(int index) {
    pthread_mutex_lock(&rc_mutex_tj2[index]);
    int r = recv_complete_tj2[index];
    pthread_mutex_unlock(&rc_mutex_tj2[index]);
    return r;
}

void add_rc_tj2(int index, int n = 1) {
    pthread_mutex_lock(&rc_mutex_tj2[index]);
    recv_complete_tj2[index] += n;
    pthread_mutex_unlock(&rc_mutex_tj2[index]);
}

size_t get_tj_num() {
	pthread_mutex_lock(&tj_num_mutex);
	size_t r = trackjoin2_num;
	pthread_mutex_unlock(&tj_num_mutex);
	return r;
}

size_t add_tj_num(int n = 1) {
	pthread_mutex_lock(&tj_num_mutex);
	trackjoin2_num += n;
	size_t r = trackjoin2_num;
	pthread_mutex_unlock(&tj_num_mutex);
	return r;
}

template<typename Table, typename Record>
class worker_param {
public:
    int tag;
    ConnectionLayer *CL;
    Table *t;
    int start_index, end_index;
    HashTable<Record> *h;
};

int TrackJoin2::get_tags() {
    return 4;
}

template<typename Table, typename Record>
static void *scan_and_send(void *param) {
    worker_param<Table, Record> *p = (worker_param<Table, Record> *) param;
    ConnectionLayer *CL = p->CL;
    int tag = p->tag;
    Table *T = p->t;
    size_t start = p->start_index;
    size_t end = p->end_index;
    HashTable<Record> *h_table = p->h;

    int hosts = CL->get_hosts();
    int local_host = CL->get_local_host();

    int dest;
    DataBlock *dbs = new DataBlock[hosts];
    // prepare data blocks for each destination
    for (dest = 0; dest < hosts; dest++) {
        while (!CL->send_begin(&dbs[dest], dest, tag));
        dbs[dest].size = 0;
    }

	//printf("Node %d scan tag %d start = %d, end = %d\n", local_host, tag, start, end);
	//fflush(stdout);

    Record *r = NULL;
    // Send each record in T to destination node
    for (size_t i = start; i < end; i++) {
        //printf("Scan - Node %d traverse #%d data block key = %u, payload = %u, tag = %d\n", local_host, i, T->records[i].k, T->records[i].p, tag);
        //fflush(stdout);
        // check if key is duplicate
        if (h_table->find(T->records[i].k, &r, h_table->getSize() + 1, 10) == h_table->getSize()) {
            dest = (T->records[i].k) % hosts;
            //printf("Scan - Node %d, dest = %d, i = %d\n", local_host, dest, i);
            //fflush(stdout);
            if (dbs[dest].size + sizeof(join_key_t) > BLOCK_SIZE) {
                //msgs are sent to process T
                //If tag is 0, it is sent from R; if tag is 1, it is sent from S
                CL->send_end(dbs[dest], dest, tag);
                //printf("Scan1 - Node %d send data block to node %d with %lu records, tag = %d\n", local_host, dest, dbs[dest].size / sizeof(join_key_t), tag);
                //fflush(stdout);
                while (!CL->send_begin(&dbs[dest], dest, tag));
                dbs[dest].size = 0;
            }
            *((join_key_t *) dbs[dest].data + dbs[dest].size / sizeof(join_key_t)) = T->records[i].k;
            dbs[dest].size += sizeof(join_key_t);
            assert(dbs[dest].size <= BLOCK_SIZE);
        }
        int pos;
        if ((pos = h_table->add(&(T->records[i]))) == h_table->getSize()) {
            printf("Scan - Node %d, hash table full! size = %lu\n", local_host, h_table->getSize());
            fflush(stdout);
        } else {
			if (tag == 0) {
				added_r_num++;
			} else if (tag == 1) {
				added_s_num++;
			}
		}
    }

	//printf("Node %d added_r_num = %lu\n", local_host, added_r_num);
	//printf("Node %d added_s_num = %lu\n", local_host, added_s_num);
	//fflush(stdout);

    // Send last partially filled data blocks to to all nodes
    for (dest = 0; dest < hosts; dest++) {
        if (dbs[dest].size > 0) {
            assert(dbs[dest].size <= BLOCK_SIZE);
            CL->send_end(dbs[dest], dest, tag);
        }
    }

	printf("Scan - Node %d scan tag %d FINISHED!!!\n", local_host, tag);
	fflush(stdout);

    return NULL;
}

static void *recv_keys(void *param) {
    //table_r here in worker_param can be any type of table. It has no use in this function.
    worker_param<table_r, record_key> *p = (worker_param<table_r, record_key> *) param;
    ConnectionLayer *CL = p->CL;
    int tag = p->tag;
    HashTable<record_key> *h_table = p->h;

    int hosts = CL->get_hosts();
    int local_host = CL->get_local_host();

    int src;
    record_key *r;
    DataBlock db;

    // Receive until termination received from all nodes
    while (recv_complete_tj2[tag] < hosts) {
        int ret;
        while ((ret = CL->recv_begin(&db, &src, tag)) == 0 && recv_complete_tj2[tag] < hosts);
        if (ret != 0) {
            assert(db.size <= BLOCK_SIZE);
            if (db.size > 0) {
                size_t bytes_copied = 0;
                while (bytes_copied < db.size) {
                    r = new record_key();
                    assert(bytes_copied + sizeof(join_key_t) <= db.size);
                    r->k = *((join_key_t *) db.data + bytes_copied / sizeof(join_key_t));
                    r->src = (uint8_t) src;
                    r->table_type = (tag == 0 ? 'R' : 'S');
                    r->visited = false;

                    //printf("recv_keys: Node %d recv_key = %u, src = %u, type = %c\n", local_host, r->k, r->src, r->table_type);
                    //fflush(stdout);
                    int pos;
                    if ((pos = h_table->add(r)) == h_table->getSize()) {
                        printf("recv_keys: Node %d, hash table full!!!, size = %lu\n", local_host, h_table->getSize());
                        fflush(stdout);
                    }
                    bytes_copied += sizeof(join_key_t);
                }
            } else {
                add_rc_tj2(tag);
            }
            CL->recv_end(db, src, tag);
        }
    }

	printf("Node %d recv_key from tag %d FINISHED!!\n", local_host, tag);
	fflush(stdout);

    return NULL;
}

static void *notify_nodes(void *param) {
    //table_r here in worker_param can be any type of table. It has no use in this function.
    worker_param<table_r, record_key> *p = (worker_param<table_r, record_key> *) param;
    ConnectionLayer *CL = p->CL;
    int tag = p->tag;
    HashTable<record_key> *h_table = p->h;

    int hosts = CL->get_hosts();
    int local_host = CL->get_local_host();

    //printf("Node %d, notify_nodes begin!!! \n", local_host);
    //fflush(stdout);

    DataBlock *dbs = new DataBlock[hosts];
    // prepare data blocks for each destination
    int dest;
    for (dest = 0; dest < hosts; dest++) {
        while (!CL->send_begin(&dbs[dest], dest, tag));
        dbs[dest].size = 0;
    }

/*
    size_t table_size = h_table->getSize();
    //size_t k_index = -1;
	size_t r_index = -1;
	//printf("r_index = %lu, r_index + 1 = %lu\n", r_index, r_index + 1); 
    join_key_t k;
    record_key *rec = NULL;
    msg_key_int m;
	size_t r_num = 0;

    // for all <k, node_r> in hash table
    while ((r_index = h_table->markNextKey(r_index + 1, &rec, 'R', true)) != table_size) {
        //printf("notify_nodes: Node %d, key_r %u\n", local_host, rec->k);
		r_num++;
        int node_r = rec->src;
        k = rec->k;
        size_t s_index = -1;
		bool found_key = false;
        // find all <k, node_s> in table
        // Do not mark keys sent from S as visited. So we pass "false" to its parameter
    	//printf("r_index = %lu, k = %lu, key = %lu, table_size = %lu\n", r_index, k, h_table->table[r_index]->k, table_size); 
		//fflush(stdout);
        while ((s_index = h_table->markVisited(k, s_index + 1, 'S', &rec, false, found_key)) != table_size) {
            int node_s = rec->src;
            printf("notify_nodes: Node %d, send <key %u, node_s %d> to node_r %d\n", local_host, k, node_s, node_r);
            fflush(stdout);
            // send <k, node_s> to node_r, use tag 2
            m.k = k;
            m.content = node_s;
            if (dbs[node_r].size + sizeof(msg_key_int) > BLOCK_SIZE) {
                CL->send_end(dbs[node_r], node_r, tag);
                while (!CL->send_begin(&dbs[node_r], node_r, tag));
                dbs[node_r].size = 0;
            }
            *((msg_key_int *) dbs[node_r].data + dbs[node_r].size / sizeof(msg_key_int)) = m;
            dbs[node_r].size += sizeof(msg_key_int);
            assert(dbs[node_r].size <= BLOCK_SIZE);
        }
    }

	printf("Node %d, r_num = %lu\n", local_host, r_num);

*/

    size_t table_size = h_table->getSize();
    size_t k_index = -1;
    join_key_t k;
    msg_key_int m;

    // for all distinct key k in hash table
    while ((k_index = h_table->getNextKey(k_index + 1, k, true)) != table_size) {
        //printf("notify_nodes: Node %d, key %u\n", local_host, k);

        bool *nodes = new bool[hosts];
        for (int i = 0; i < hosts; i++) {
            nodes[i] = false;
        }

        // find all <k, node_s> in table
        size_t s_index = k_index - 1;
        record_key *r = NULL;
        int node_s = -1;
        while ((s_index = h_table->markVisited(k, s_index + 1, 'S', &r, true)) != table_size) {
            node_s = r->src;
            assert(node_s >= 0);
            nodes[node_s] = true;
			int true_num = 0;
			for (int i = 0; i < hosts; i++) {
				true_num += nodes[i];
			}
			if (true_num == hosts) {
				break;
			}
        }

        size_t r_index = k_index - 1;
        // for all <k, node_r> in table
        while ((r_index = h_table->markVisited(k, r_index + 1, 'R', &r, true)) != table_size) {
            //printf("notify_nodes: Node %d, r_index = %lu, node_r = %d\n", local_host, r_index, node_r);
            //fflush(stdout);
            // for all <k, node_s> in table
            int node_r = r->src;
            for (node_s = 0; node_s < hosts; node_s++) {
                if (nodes[node_s]) {
                    //printf("notify_nodes: Node %d, s_index = %lu, node_s = %d\n", local_host, s_index, node_s);
                    //fflush(stdout);
                    // send <k, node_s> to node_r, use tag 2
                    m.k = k;
                    m.content = node_s;
                    if (dbs[node_r].size + sizeof(msg_key_int) > BLOCK_SIZE) {
                        CL->send_end(dbs[node_r], node_r, tag);
                        while (!CL->send_begin(&dbs[node_r], node_r, tag));
                        dbs[node_r].size = 0;
                    }
                    //printf("notify_nodes: Node %d, send <key %u, node_s %d> to node_r %d\n", local_host, k, node_s, node_r);
                    //fflush(stdout);
                    *((msg_key_int *) dbs[node_r].data + dbs[node_r].size / sizeof(msg_key_int)) = m;
                    dbs[node_r].size += sizeof(msg_key_int);
                    assert(dbs[node_r].size <= BLOCK_SIZE);
                }
            }
        }
    }

    // Send last partially filled data blocks to all nodes
    for (dest = 0; dest < hosts; dest++) {
        if (dbs[dest].size > 0) {
            // Send last data blocks
            assert(dbs[dest].size <= BLOCK_SIZE);
            CL->send_end(dbs[dest], dest, tag);
        }
    }

    printf("Node %d notify_nodes FINISHED \n", local_host);
    fflush(stdout);

    return NULL;
}

// R (use tag 3 to send)
static void *send_tuple(void *param) {
    worker_param<table_r, record_r> *p = (worker_param<table_r, record_r> *) param;
    ConnectionLayer *CL = p->CL;
    int tag = p->tag;
    HashTable<record_r> *h_table = p->h;

    int hosts = CL->get_hosts();
    int local_host = CL->get_local_host();

    int src;
    DataBlock db_recv;
    msg_key_int m;
    DataBlock *db_send = new DataBlock[hosts];

    int dest;
    for (dest = 0; dest < hosts; dest++) {
        while (!CL->send_begin(&db_send[dest], dest, tag));
        db_send[dest].size = 0;
    }

    while (recv_complete_tj2[2] < hosts) {
        //receive send_tuple notification from process T in tag 2
        int ret = 0;
        while ((ret = CL->recv_begin(&db_recv, &src, 2)) == 0 && recv_complete_tj2[2] < hosts);
        if (ret != 0) {
            assert(db_recv.size <= BLOCK_SIZE);
            if (db_recv.size > 0) {
                size_t bytes_copied = 0;
                while (bytes_copied < db_recv.size) {
                    m = *((msg_key_int *) db_recv.data + bytes_copied / sizeof(msg_key_int));
                    bytes_copied += sizeof(msg_key_int);
                    join_key_t k = m.k;
                    dest = m.content;

                    //printf("Node %d send_tuple: key %u, dest %d\n", local_host, k, dest);
                    //fflush(stdout);

                    record_r *r = new record_r();
                    size_t r_index = h_table->getSize();
                    while ((r_index = h_table->find(k, &r, r_index + 1, 10)) != h_table->getSize()) {
                        //printf("Node %d send_tuple: r_index %lu, key %u, payload %u to Node %d\n", local_host, r_index, r->k, r->p, dest);
                        //fflush(stdout);
                        if (db_send[dest].size + sizeof(record_r) > BLOCK_SIZE) {
                            CL->send_end(db_send[dest], dest, tag);
                            while (!CL->send_begin(&db_send[dest], dest, tag));
                            db_send[dest].size = 0;
                        }
                        *((record_r *) db_send[dest].data + db_send[dest].size / sizeof(record_r)) = *r;
                        db_send[dest].size += sizeof(record_r);
                        assert(db_send[dest].size <= BLOCK_SIZE);
                    }
                }
            } else {
                add_rc_tj2(2);
            }
            CL->recv_end(db_recv, src, 2);
        }
    }

    // Send last partially filled data blocks and end flags to all nodes
    // tag 3
    for (dest = 0; dest < hosts; dest++) {
        if (db_send[dest].size > 0) {
            // Send last data blocks
            assert(db_send[dest].size <= BLOCK_SIZE);
            CL->send_end(db_send[dest], dest, tag);
        }
    }

//    printf("Node %d send_tuple FINISHED\n", local_host);
//    fflush(stdout);

    return NULL;
}

// S
static void *join_tuple(void *param) {
    worker_param<table_s, record_s> *p = (worker_param<table_s, record_s> *) param;
    ConnectionLayer *CL = p->CL;
    int tag = p->tag;
    HashTable<record_s> *h_table = p->h;

    int hosts = CL->get_hosts();
    int local_host = CL->get_local_host();

//    printf("Node %d, join_tuple begin!!!, tag = %d\n", local_host, tag);
//    fflush(stdout);

    int src;
    DataBlock db_recv;

    record_r *r;
    record_s *s = NULL;

    while (recv_complete_tj2[3] < hosts) {
        int ret;
        while ((ret = CL->recv_begin(&db_recv, &src, tag)) == 0 && recv_complete_tj2[3] < hosts) {
			//printf("Node %d I am here!!, tag %d, t = %d\n", local_host, tag, t);
		}
        if (ret != 0) {
            assert(db_recv.size <= BLOCK_SIZE);
            if (db_recv.size > 0) {
                size_t bytes_copied = 0;
                while (bytes_copied < db_recv.size) {
                    r = new record_r();
                    assert(bytes_copied + sizeof(record_r) <= db_recv.size);
                    *r = *((record_r *) db_recv.data + bytes_copied / sizeof(record_r));
                    recv_tuples++;
					size_t pos = h_table->getSize();
                    //printf("Node %d, here 2\n", local_host);
                    while ((pos = h_table->find(r->k, &s, pos + 1, 10)) != h_table->getSize()) {
                        bool valid = true;
                        //printf("Join Result: Node %d #%lu, src %d, join_key %u payload_r %u, payload_s %u %s\n", local_host,
                        //		add_tj_num(), src, s->k, r->p, s->p, valid ? "correct" : "incorrect");
                        //fflush(stdout);
                        add_tj_num();
                    }
                    bytes_copied += sizeof(record_r);
                }
            } else {
                add_rc_tj2(3);
            }
            CL->recv_end(db_recv, src, tag);
        }
    }

//    printf("Node %d join_tuple FINISHED\n", local_host);
//    fflush(stdout);

    return NULL;
}

int TrackJoin2::run(ConnectionLayer *CL, struct table_r *R, struct table_s *S) {
    int t;
    worker_threads = new pthread_t[CPU_CORES];
    int local_host = CL->get_local_host();

    size_t h_table_r_size = R->num_records / 0.5;
    size_t h_table_s_size = S->num_records / 0.5;
    size_t h_table_key_size = (R->num_records + S->num_records) / 0.5;

    printf("Node %d, R hash table size = %lu, S hash table size = %lu\n", local_host, h_table_r_size, h_table_s_size);
    fflush(stdout);

    HashTable<record_r> *h_table_r = new HashTable<record_r>(h_table_r_size);
    HashTable<record_s> *h_table_s = new HashTable<record_s>(h_table_s_size);
    HashTable<record_key> *h_table_key = new HashTable<record_key>(h_table_key_size);

    printf("Node %d, after create 3 hash tables\n", local_host);
    fflush(stdout);

    int times, timed;

    times = time(NULL);

    size_t r_interval = R->num_records / R_SCAN_THREADS;
    // start R table scan_and_send
    for (t = 0; t < R_SCAN_THREADS; t++) {
        worker_param<table_r, record_r> *param_r = new worker_param<table_r, record_r>();
        param_r->CL = CL;
        param_r->t = R;
        param_r->tag = 0;
        param_r->h = h_table_r;
        param_r->start_index = r_interval * t;
        param_r->end_index = r_interval * (t + 1);
        pthread_create(&worker_threads[t], NULL, &scan_and_send<table_r, record_r>, (void *) param_r);
    }

    size_t s_interval = S->num_records / S_SCAN_THREADS;
    // start S table scan_and_send
    for (; t < R_SCAN_THREADS + S_SCAN_THREADS; t++) {
        worker_param<table_s, record_s> *param_s = new worker_param<table_s, record_s>();
        param_s->CL = CL;
        param_s->t = S;
        param_s->tag = 1;
        param_s->h = h_table_s;
        param_s->start_index = s_interval * (t - R_SCAN_THREADS);
        param_s->end_index = s_interval * (t - R_SCAN_THREADS + 1);
        pthread_create(&worker_threads[t], NULL, &scan_and_send<table_s, record_s>, (void *) param_s);
    }

    for (int i = 0; i < 4; i++) {
        pthread_mutex_init(&rc_mutex_tj2[i], NULL);
        recv_complete_tj2[i] = 0;
    }
	pthread_mutex_init(&tj_num_mutex, NULL);

    // start process receiving keys sent from R
    for (; t < R_SCAN_THREADS + S_SCAN_THREADS + RECV_R_THREADS; t++) {
        worker_param<table_r, record_key> *param_t = new worker_param<table_r, record_key>();
        param_t->CL = CL;
        param_t->tag = 0;
        param_t->h = h_table_key;
        pthread_create(&worker_threads[t], NULL, &recv_keys, (void *) param_t);
    }

    // start process receiving keys sent from S
    for (; t < CPU_CORES; t++) {
        worker_param<table_r, record_key> *param_t = new worker_param<table_r, record_key>();
        param_t->CL = CL;
        param_t->tag = 1;
        param_t->h = h_table_key;
        pthread_create(&worker_threads[t], NULL, &recv_keys, (void *) param_t);
    }

    // barrier for R scan_and_send
    for (t = 0; t < R_SCAN_THREADS; t++) {
        void *retval;
        pthread_join(worker_threads[t], &retval);
    }

    int hosts = CL->get_hosts();
    DataBlock *dbs = new DataBlock[hosts];
    for (int dest = 0; dest < hosts; dest++) {
        while (!CL->send_begin(&dbs[dest], dest, 0));
        dbs[dest].size = 0;
        CL->send_end(dbs[dest], dest, 0);
        printf("Scan3 - Node %d send end flag node %d with tag 0\n", local_host, dest);
        fflush(stdout);
    }

	int time_r_scan = time(NULL);
 	printf("Node %d scan table r taken: %ds\n", local_host, time_r_scan - times);

   // barrier for S scan_and_send
    for (; t < R_SCAN_THREADS + S_SCAN_THREADS; t++) {
        void *retval;
        pthread_join(worker_threads[t], &retval);
    }

    for (int dest = 0; dest < hosts; dest++) {
        while (!CL->send_begin(&dbs[dest], dest, 1));
        dbs[dest].size = 0;
        CL->send_end(dbs[dest], dest, 1);
        printf("Scan3 - Node %d send end flag node %d with tag 1\n", local_host, dest);
        fflush(stdout);
    }

	int time_s_scan = time(NULL);
	printf("Node %d scan table s taken: %ds\n", local_host, time_s_scan - times);

    // barrier
    for (; t < CPU_CORES; t++) {
        void *retval;
        pthread_join(worker_threads[t], &retval);
    }
	
	int time_recv = time(NULL);
	printf("Node %d receive keys taken: %ds\n", local_host, time_recv - times);

    printf("Node %d, h_table_key size = %lu\n", local_host, h_table_key->getSize());
    fflush(stdout);
    //h_table_key->printAll(local_host);

    for (t = 0; t < T_NOTI_THREADS; t++) {
        // notify R processes
        worker_param<table_r, record_key> *param_t = new worker_param<table_r, record_key>();
        param_t->CL = CL;
        param_t->tag = 2;
        param_t->h = h_table_key;
        pthread_create(&worker_threads[t], NULL, &notify_nodes, (void *) param_t);
    }

    // R receives notification and send its tuples to S
    // listening on tag 2 while sending with tag 3
    for (; t < T_NOTI_THREADS + R_SEND_THREADS; t++) {
        worker_param<table_r, record_r> *param_r = new worker_param<table_r, record_r>();
        param_r->CL = CL;
        param_r->tag = 3;
        param_r->h = h_table_r;
        pthread_create(&worker_threads[t], NULL, &send_tuple, (void *) param_r);
    }

    // S receives key sent from R and join the tuples
    for (; t < CPU_CORES; t++) {
        worker_param<table_s, record_s> *param_s = new worker_param<table_s, record_s>();
        param_s->CL = CL;
        param_s->tag = 3;
        param_s->h = h_table_s;
        pthread_create(&worker_threads[t], NULL, &join_tuple, (void *) param_s);
    }

    // barrier
    for (t = 0; t < T_NOTI_THREADS; t++){
        void *retval;
        pthread_join(worker_threads[t], &retval);
    }

    for (int dest = 0; dest < hosts; dest++) {
        while (!CL->send_begin(&dbs[dest], dest, 2));
        dbs[dest].size = 0;
        CL->send_end(dbs[dest], dest, 2);
        printf("notify_nodes: Node %d send end flag to node %d with tag 2\n", local_host, dest);
        fflush(stdout);
    }

 	int time_noti = time(NULL);
	printf("Node %d notify nodes taken: %ds\n", local_host, time_noti - time_recv);

   // barrier
    for (; t < T_NOTI_THREADS + R_SEND_THREADS; t++) {
        void *retval;
        pthread_join(worker_threads[t], &retval);
    }

    for (int dest = 0; dest < hosts; dest++) {
        while (!CL->send_begin(&dbs[dest], dest, 3));
        dbs[dest].size = 0;
        CL->send_end(dbs[dest], dest, 3);
        printf("send_tuple: Node %d send end flag to dest %d with tag 3\n", local_host, dest);
        fflush(stdout);
    }
	
	int time_send = time(NULL);
	printf("Node %d send tuples taken: %ds\n", local_host, time_send - time_recv);

    // barrier for join_tuple threads
    for (; t < CPU_CORES; t++) {
        void *retval;
        pthread_join(worker_threads[t], &retval);
    }
	
	int time_join = time(NULL);
	printf("Node %d join tuples taken: %ds\n", local_host, time_join - time_recv);


    printf("Node %d JOIN NUM = %lu, recv_tuples = %lu\n", local_host, trackjoin2_num, recv_tuples);
    fflush(stdout);

    timed = time(NULL);
    printf("Node %d Total time taken: %ds\n", local_host, timed - times);

    delete h_table_r;
    delete h_table_s;
    delete h_table_key;

    return 0;
}
