#include "Algorithms.h"
#include "ConnectionLayer.h"
#include "usertype.h"
#include "HashTable.h"
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>

size_t trackjoin2_num = 0;

template <typename Table, typename Record>
class worker_param {
public:
    int tag;
    ConnectionLayer *CL;
    Table *t;
    int start_index, end_index;
    HashTable<Record> *h;
};

int TrackJoin2::get_tags() {
    return 3;
}

template <typename Table, typename Record>
static void *scan_and_send(void *param) {
    worker_param<Table, Record> *p = (worker_param<Table, Record> *) param;
    ConnectionLayer *CL = p->CL;
    int tag = p->tag;
    Table *T = p->t;
    HashTable<Record> *h_table = p->h;

    int hosts = CL->get_hosts();

    int dest;
    DataBlock *dbs = new DataBlock[hosts];
    // prepare data blocks for each destination
    for (dest = 0; dest < hosts; dest++) {
        while (!CL->send_begin(&dbs[dest], dest, tag));
        dbs[dest].size = 0;
    }

    Record *r = NULL;
    // Send each record in T to destination node
    for (int i = 0; i < T->num_records; i++) {
        // check if key is duplicate
        if (h_table->find(T->records[i].k, &r, h_table->getSize() + 1, 10) == h_table->getSize()) {
            dest = h_table->hash32(T->records[i].k) % hosts;
            if (dbs[dest].size + sizeof(join_key_t) > BLOCK_SIZE) {
                //msgs are sent to process T
                //If tag is 0, it is sent from R; if tag is 1, it is sent from S
                CL->send_end(dbs[dest], dest, tag);
                //printf("Scan - Node %d send data block to node %d with %lu records\n", local_host, dest, dbs[dest].size / sizeof(Record));
                //fflush(stdout);
                while (!CL->send_begin(&dbs[dest], dest, tag));
                dbs[dest].size = 0;
            }
            *((join_key_t *) dbs[dest].data + dbs[dest].size / sizeof(join_key_t)) = T->records[i].k;
            dbs[dest].size += sizeof(join_key_t);
            assert(dbs[dest].size <= BLOCK_SIZE);
        }
        h_table->add(&(T->records[i]));
    }

    // Send last partially filled data blocks and end flags to all nodes
    for (dest = 0; dest < hosts; dest++) {
        // Send last data blocks
        if (dbs[dest].size > 0) {
            assert(dbs[dest].size <= BLOCK_SIZE);
            CL->send_end(dbs[dest], dest, tag);
            //printf("Scan - Node %d send data block to node %d with %lu records\n", local_host, dest, dbs[dest].size / sizeof(Record));
            //fflush(stdout);
        }
        // Send end flags
        while (!CL->send_begin(&dbs[dest], dest, tag));
        dbs[dest].size = 0;
        CL->send_end(dbs[dest], dest, tag);
        //printf("Scan - Node %d send end flag node %d with size %lu\n", local_host, dest, dbs[dest].size);
        //fflush(stdout);
    }

    return NULL;
}

static void *recv_keys(void *param) {
    //table_r here in worker_param can be any type of table. It has no use in this function.
    worker_param<table_r, record_key> *p = (worker_param<table_r, record_key> *) param;
    ConnectionLayer *CL = p->CL;
    int tag = p->tag;
    HashTable<record_key> *h_table = p->h;

    int hosts = CL->get_hosts();
    //int local_host = CL->get_local_host();

    int src;
    record_key *r;
    DataBlock db;

    int t = 0;
    // Receive until termination received from all nodes
    while (t != hosts) {
        while (!CL->recv_begin(&db, &src, tag));
        assert(db.size <= BLOCK_SIZE);
        if (db.size > 0) {
            size_t bytes_copied = 0;
            while (bytes_copied < db.size) {
                r = new record_key();
                assert(bytes_copied + sizeof(join_key_t) <= db.size);
                memcpy(&r->k, db.data, sizeof(join_key_t));
                r->src = src;
                r->table_type = (tag == 0 ? 'R' : 'S');

                h_table->add(r);

                bytes_copied += sizeof(join_key_t);
            }
        } else {
            t++;
        }
        CL->recv_end(db, src, tag);
    }

	return NULL;
}

static void *notify_nodes(void *param) {
    //table_r here in worker_param can be any type of table. It has no use in this function.
    worker_param<table_r, record_key> *p = (worker_param<table_r, record_key> *) param;
    ConnectionLayer *CL = p->CL;
    int tag = p->tag;
    HashTable<record_key> *h_table = p->h;

    int hosts = CL->get_hosts();
    //int local_host = CL->get_local_host();

    DataBlock *dbs = new DataBlock[hosts];
    // prepare data blocks for each destination
    int dest;
    for (dest = 0; dest < hosts; dest++) {
        while (!CL->send_begin(&dbs[dest], dest, tag));
        dbs[dest].size = 0;
    }

    size_t table_size = h_table->getSize();
    size_t k_index = -1;
    join_key_t k;
    // for all distinct key k in hash table
    while ((k_index = h_table->getNextKey(k_index + 1, k)) != table_size) {
        size_t r_index = k_index - 1;
        int node_r = -1;
        // for all <k, node_r> in table
        while ((r_index = h_table->markUsed(k, r_index + 1, 'R', node_r, 10)) != table_size) {
            size_t s_index = k_index - 1;
            int node_s = -1;
            // for all <k, node_s> in table
            while ((s_index = h_table->markUsed(k, s_index + 1, 'S', node_s, 10)) != table_size) {
                // send <k, node_s> to node_r, use tag 2
                if (dbs[node_r].size + sizeof(join_key_t) + sizeof(int) > BLOCK_SIZE) {
                    CL->send_end(dbs[node_r], node_r, tag);
                    while (!CL->send_begin(&dbs[node_r], node_r, tag));
                    dbs[node_r].size = 0;
                }
                *((join_key_t *) dbs[node_r].data) = k;
                memcpy((char *) dbs[node_r].data + sizeof(join_key_t), &node_s, sizeof(int));
            }
        }
    }

    // Send last partially filled data blocks and end flags to all nodes
    for (dest = 0; dest < hosts; dest++) {
        // Send last data blocks
        if (dbs[dest].size > 0) {
            assert(dbs[dest].size <= BLOCK_SIZE);
            CL->send_end(dbs[dest], dest, tag);
            //printf("Scan - Node %d send data block to node %d with %lu records\n", local_host, dest, dbs[dest].size / sizeof(Record));
            //fflush(stdout);
        }
        // Send end flags
        while (!CL->send_begin(&dbs[dest], dest, tag));
        dbs[dest].size = 0;
        CL->send_end(dbs[dest], dest, tag);
        //printf("Scan - Node %d send end flag node %d with size %lu\n", local_host, dest, dbs[dest].size);
        //fflush(stdout);
    }

    return NULL;
}

// R (use tag 3)
static void *send_tuple(void *param) {
    worker_param<table_r, record_r> *p = (worker_param<table_r, record_r> *) param;
    ConnectionLayer *CL = p->CL;
    int tag = p->tag;
    HashTable<record_r> *h_table = p->h;

    int hosts = CL->get_hosts();

    int src;
    DataBlock db_recv;
    DataBlock *db_send = new DataBlock[hosts];

    int dest;
    for (dest = 0; dest < hosts; dest++) {
        while (!CL->send_begin(&db_send[dest], dest, tag));
        db_send[dest].size = 0;
    }

    int t = 0;
    while (t != hosts) {
        while (!CL->recv_begin(&db_recv, &src, 2));
        assert(db_recv.size <= BLOCK_SIZE);
        if (db_recv.size > 0) {
            size_t bytes_copied = 0;
            while (bytes_copied < db_recv.size) {
                join_key_t k = *((join_key_t *) db_recv.data);
                // set destination node node_s
                memcpy(&dest, (char *)db_recv.data + sizeof(join_key_t), sizeof(int));
                bytes_copied += sizeof(join_key_t) + sizeof(int);
                record_r *r = new record_r();
                size_t r_index = h_table->getSize();
                while ((r_index = h_table->find(k, &r, r_index + 1, 10)) != h_table->getSize()) {
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
            t++;
        }
        CL->recv_end(db_recv, src, 2);
    }

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

    int src;
    DataBlock db_recv;

    record_r *r;
    record_s *s = NULL;

    int t = 0;
    while (t != hosts) {
        while (!CL->recv_begin(&db_recv, &src, tag));
        assert(db_recv.size <= BLOCK_SIZE);
        if (db_recv.size > 0) {
            size_t bytes_copied = 0;
            while (bytes_copied < db_recv.size) {
                r = new record_r();
                assert(bytes_copied + sizeof(record_s) <= db_recv.size);
                *r = *((record_r *) db_recv.data + bytes_copied / sizeof(record_r));
                size_t pos = h_table->getSize();
                while ((pos = h_table->find(r->k, &s, pos + 1, 10)) != h_table->getSize()) {
                    bool valid = true;
                    printf("Join Result: Node %d #%lu, join_key %u payload_r %u, payload_s %u %s\n", local_host, trackjoin2_num,
                            s->k, r->p, s->p, valid ? "correct" : "incorrect");
                    fflush(stdout);
                    trackjoin2_num++;
                }
                bytes_copied += sizeof(record_r);
            }
        } else {
            t++;
        }
        CL->recv_end(db_recv, src, tag);
    }

    return NULL;
}

int TrackJoin2::run(ConnectionLayer * CL, struct table_r *R, struct table_s *S) {
    int t;
    worker_threads = new pthread_t[32];

    size_t h_table_r_size = R->num_records / 0.5;
    size_t h_table_s_size = S->num_records / 0.5;
    size_t h_table_key_size = (R->num_records + S->num_records) / 0.5;

    printf("R hash table size = %lu, S hash table size = %lu\n", h_table_r_size, h_table_s_size);
    fflush(stdout);

    HashTable<record_r> *h_table_r = new HashTable<record_r>(h_table_r_size);
    HashTable<record_s> *h_table_s = new HashTable<record_s>(h_table_s_size);
    HashTable<record_key> *h_table_key = new HashTable<record_key>(h_table_key_size);

    // start R table scan_and_send
    worker_param<table_r, record_r> *param_r = new worker_param<table_r, record_r>();
    param_r->CL = CL;
    param_r->t = R;
    param_r->tag = 0;
    param_r->h = h_table_r;
    pthread_create(&worker_threads[0], NULL, &scan_and_send<table_r, record_r>, (void *) param_r);

    // start S table scan_and_send
    worker_param<table_s, record_s> *param_s = new worker_param<table_s, record_s>();
    param_s->CL = CL;
    param_s->t = S;
    param_s->tag = 1;
    param_s->h = h_table_s;
    pthread_create(&worker_threads[1], NULL, &scan_and_send<table_s, record_s>, (void *) param_s);

    // start 2 process T recv_keys to receive keys sent from R and S respectively
    for (t = 0; t < 2; t++) {
        worker_param<table_r, record_key> *param_t = new worker_param<table_r, record_key>();
        param_t->CL = CL;
        param_t->tag = t;
        param_t->h = h_table_key;
        pthread_create(&worker_threads[t + 2], NULL, &recv_keys, (void *) param_t);
    }

    // barrier
    for (t = 0; t < 4; t++) {
        void *retval;
        pthread_join(worker_threads[t], &retval);
    }

    // notify R processes
    worker_param<table_r, record_key> *param_t = new worker_param<table_r, record_key>();
    param_t->CL = CL;
    param_t->tag = 2;
    param_t->h = h_table_key;
    pthread_create(&worker_threads[0], NULL, &notify_nodes, (void *) param_t);

    // R receives notification and send its tuples to S
    // listening on tag 2 while sending with tag 3
    param_r = new worker_param<table_r, record_r>();
    param_r->CL = CL;
    param_r->tag = 3;
    param_r->h = h_table_r;
    pthread_create(&worker_threads[1], NULL, &send_tuple, (void *) param_r);

    // S receives key sent from R and join the tuples
    param_s = new worker_param<table_s, record_s>();
    param_s->CL = CL;
    param_s->tag = 3;
    param_s->h = h_table_s;
    pthread_create(&worker_threads[2], NULL, &join_tuple, (void *) param_s);

    // barrier
    for (t = 0; t < 3; t++) {
        void *retval;
        pthread_join(worker_threads[t], &retval);
    }


    return 0;
}