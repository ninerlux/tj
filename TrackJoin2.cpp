#include "Algorithms.h"
#include "ConnectionLayer.h"
#include "usertype.h"
#include "HashTable.h"
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <unistd.h>

size_t trackjoin2_num = 0;

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

    Record *r = NULL;
    // Send each record in T to destination node
    for (size_t i = 0; i < T->num_records; i++) {
        // check if key is duplicate
        //printf("Scan - Node %d traverse data block key = %u, payload = %u, tag = %d\n", local_host, T->records[i].k, T->records[i].p, tag);
        //fflush(stdout);
        if (h_table->find(T->records[i].k, &r, h_table->getSize() + 1, 10) == h_table->getSize()) {
            dest = h_table->hash32(T->records[i].k) % hosts;
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
            printf("Scan - Node %d, hash table full!\n", local_host);
            fflush(stdout);
        }
    }

    // Send last partially filled data blocks and end flags to all nodes
    for (dest = 0; dest < hosts; dest++) {
        if (dbs[dest].size == 0) {
            // Send last data blocks
            CL->send_end(dbs[dest], dest, tag);
            //printf("Scan3 - Node %d send end flag node %d with size %lu, tag = %d\n", local_host, dest, dbs[dest].size, tag);
            //fflush(stdout);
        } else {
            assert(dbs[dest].size <= BLOCK_SIZE);
            CL->send_end(dbs[dest], dest, tag);
            //printf("Scan2 - Node %d send data block to node %d with %lu records, tag = %d\n", local_host, dest, dbs[dest].size / sizeof(join_key_t), tag);
            //fflush(stdout);
            // Send end flags
            while (!CL->send_begin(&dbs[dest], dest, tag));
            dbs[dest].size = 0;
            CL->send_end(dbs[dest], dest, tag);
            //printf("Scan3 - Node %d send end flag node %d with size %lu, tag = %d\n", local_host, dest, dbs[dest].size, tag);
            //fflush(stdout);
        }
    }

    printf("Scan - Node %d scan FINISHED!!\n", local_host);
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
                r->k = *((join_key_t *) db.data + bytes_copied / sizeof(join_key_t));
                r->src = (uint8_t) src;
                r->table_type = (tag == 0 ? 'R' : 'S');
                r->visited = false;

                //printf("recv_keys: Node %d recv_key = %u, src = %u, type = %c\n", local_host, r->k, r->src, r->table_type);
                //fflush(stdout);
                int pos;
                if ((pos = h_table->add(r)) == h_table->getSize()) {
                    printf("recv_keys: Node %d, hash table full!!!\n", local_host);
                    fflush(stdout);
                }
                //printf("recv_keys: Node %d, res = %lu, table_r_k %u\n", local_host, res, h_table->table[res]->k);
                //fflush(stdout);

                bytes_copied += sizeof(join_key_t);
            }
        } else {
            t++;
        }
        CL->recv_end(db, src, tag);
    }

    printf("Node %d recv_key from tag %d FINISHED \n", local_host, tag);
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

    // Send last partially filled data blocks and end flags to all nodes
    for (dest = 0; dest < hosts; dest++) {
        if (dbs[dest].size == 0) {
            // Send end flags
            CL->send_end(dbs[dest], dest, tag);
        } else {
            // Send last data blocks
            assert(dbs[dest].size <= BLOCK_SIZE);
            CL->send_end(dbs[dest], dest, tag);
            // Send end flags
            while (!CL->send_begin(&dbs[dest], dest, tag));
            dbs[dest].size = 0;
            CL->send_end(dbs[dest], dest, tag);
            //printf("notify_nodes: send end flag to node %d\n", dest);
            //fflush(stdout);
        }
    }

    printf("Node %d notify_nodes FINISHED \n", local_host);
    fflush(stdout);

    return NULL;
}

// R (use tag 3)
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

    int t = 0;
    while (t != hosts) {
        //receive send_tuple notification from process T in tag 2
        while (!CL->recv_begin(&db_recv, &src, 2));
        assert(db_recv.size <= BLOCK_SIZE);
        if (db_recv.size > 0) {
            size_t bytes_copied = 0;
            while (bytes_copied < db_recv.size) {
                //join_key_t k = *((join_key_t *) db_recv.data);
                // set destination node node_s
                //memcpy(&dest, (char *)db_recv.data + sizeof(join_key_t), sizeof(int));
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
            t++;
        }
        //send_tuple notification in tag 2
        CL->recv_end(db_recv, src, 2);
    }

    // Send last partially filled data blocks and end flags to all nodes
    // tag 3
    for (dest = 0; dest < hosts; dest++) {
        if (db_send[dest].size == 0) {
            // Send end flags
            CL->send_end(db_send[dest], dest, tag);
            printf("send_tuple: Node %d send end flag to dest %d with tag %d\n", local_host, dest, tag);
            fflush(stdout);
        } else {
            // Send last data blocks
            assert(db_send[dest].size <= BLOCK_SIZE);
            CL->send_end(db_send[dest], dest, tag);
            // Send end flags
            while (!CL->send_begin(&db_send[dest], dest, tag));
            db_send[dest].size = 0;
            CL->send_end(db_send[dest], dest, tag);
            printf("send_tuple: Node %d send end flag to dest %d with tag %d\n", local_host, dest, tag);
            fflush(stdout);
        }
    }

    printf("Node %d send_tuple FINISHED\n", local_host);
    fflush(stdout);

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

    //printf("Node %d, join_tuple begin!!!, tag = %d\n", local_host, tag);
    //fflush(stdout);

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
                assert(bytes_copied + sizeof(record_r) <= db_recv.size);
                *r = *((record_r *) db_recv.data + bytes_copied / sizeof(record_r));
                size_t pos = h_table->getSize();

                while ((pos = h_table->find(r->k, &s, pos + 1, 10)) != h_table->getSize()) {
                    bool valid = true;
                    //printf("Join Result: Node %d #%lu, src %d, join_key %u payload_r %u, payload_s %u %s\n", local_host, src, 
                    //		trackjoin2_num, s->k, r->p, s->p, valid ? "correct" : "incorrect");
                    //fflush(stdout);
                    trackjoin2_num++;
                }
                bytes_copied += sizeof(record_r);
            }
        } else {
            t++;
            printf("join tuple : Node %d recv end flag from Node %d with tag %d\n", local_host, src, tag);
            fflush(stdout);
        }
        CL->recv_end(db_recv, src, tag);
    }

    printf("Node %d join_tuple FINISHED\n", local_host);
    fflush(stdout);

    return NULL;
}

int TrackJoin2::run(ConnectionLayer *CL, struct table_r *R, struct table_s *S) {
    int t;
    worker_threads = new pthread_t[32];
    int local_host = CL->get_local_host();

    size_t h_table_r_size = R->num_records / 0.5;
    size_t h_table_s_size = S->num_records / 0.5;
    size_t h_table_key_size = (R->num_records + S->num_records) / 0.5;

    printf("Node %d, R hash table size = %lu, S hash table size = %lu\n", local_host, h_table_r_size, h_table_s_size);
    fflush(stdout);

    HashTable<record_r> *h_table_r = new HashTable<record_r>(h_table_r_size);
    HashTable<record_s> *h_table_s = new HashTable<record_s>(h_table_s_size);
    HashTable<record_key> *h_table_key = new HashTable<record_key>(h_table_key_size);

    //printf("Node %d, after create 3 hash tables\n", local_host);
    //fflush(stdout);

    int times, timed;

    times = time(NULL);

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

    printf("Node %d, h_table_key size = %lu\n", local_host, h_table_key->getSize());
    fflush(stdout);
    //h_table_key->printAll(local_host);

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

    printf("Node %d JOIN NUM = %lu\n", local_host, trackjoin2_num);
    fflush(stdout);

    timed = time(NULL);
    printf("Total time taken: %ds\n", timed - times);

    delete h_table_r;
    delete h_table_s;
    delete h_table_key;

    return 0;
}
