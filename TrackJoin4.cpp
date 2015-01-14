#include "Algorithms.h"
#include "ConnectionLayer.h"
#include "usertype.h"
#include "HashTable.h"
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <unistd.h>

size_t trackjoin4_num = 0;

template<typename Table, typename Record>
class worker_param {
public:
    int tag;
    ConnectionLayer *CL;
    Table *t;
    int start_index, end_index;
    HashTable<Record> *h;
};

int TrackJoin4::get_tags() {
    return 7;
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

    DataBlock *dbs = new DataBlock[hosts];
    // prepare data blocks for each destination
    int dest;
    for (dest = 0; dest < hosts; dest++) {
        while (!CL->send_begin(&dbs[dest], dest, tag));
        dbs[dest].size = 0;
    }

    // Add all <k, payload> to hash table
    for (size_t i = 0; i < T->num_records; i++) {
        printf("Scan - Node %d traverse data #%lu key = %u\n", local_host, i, T->records[i].k);
        fflush(stdout);
        h_table->add(&(T->records[i]));
    }

    // Find all distinct key in hash table and send
    size_t table_size = h_table->getSize();
    size_t k_index = -1;
    join_key_t k;
    msg_key_int m;
    Record *r = NULL;

    while ((k_index = h_table->getNextKey(k_index + 1, k, true)) != table_size) {
        int c = 0;
        size_t mark_index = -1;
        while ((mark_index = h_table->markVisited(k, mark_index + 1, 'N', &r, true) != table_size)) {
            c++;
        }
        dest = h_table->hash32(k) % hosts;
        // send <k, c> to dest
        m.k = k;
        m.content = c;
        printf("Scan - Node %d send data key = %u, count = %d, tag = %d\n", local_host, k, c, tag);
        fflush(stdout);
        if (dbs[dest].size + sizeof(msg_key_int) > BLOCK_SIZE) {
            CL->send_end(dbs[dest], dest, tag);
            while (!CL->send_begin(&dbs[dest], dest, tag));
            dbs[dest].size = 0;
        }
        *((msg_key_int *) dbs[dest].data + dbs[dest].size / sizeof(msg_key_int)) = m;
        dbs[dest].size += sizeof(msg_key_int);
        assert(dbs[dest].size <= BLOCK_SIZE);
    }

    // Send last partially filled data blocks and end flags to all nodes
    for (dest = 0; dest < hosts; dest++) {
        if (dbs[dest].size == 0) {
            CL->send_end(dbs[dest], dest, tag);
        } else {
            // Send last data blocks
            assert(dbs[dest].size <= BLOCK_SIZE);
            CL->send_end(dbs[dest], dest, tag);
            // Send end flags
            while (!CL->send_begin(&dbs[dest], dest, tag));
            dbs[dest].size = 0;
            CL->send_end(dbs[dest], dest, tag);
        }
    }

    return NULL;
}

static void *recv_key_count(void *param) {
    //table_r here in worker_param can be any type of table. It has no use in this function.
    worker_param<table_r, record_key_count> *p = (worker_param<table_r, record_key_count> *) param;
    ConnectionLayer *CL = p->CL;
    int tag = p->tag;
    HashTable<record_key_count> *h_table = p->h;

    int hosts = CL->get_hosts();
    // int local_host = CL->get_local_host();

    int src;
    record_key_count *r;
    msg_key_int m;
    DataBlock db;

    int t = 0;
    // Receive until termination received from all nodes
    while (t != hosts) {
        while (!CL->recv_begin(&db, &src, tag));
        assert(db.size <= BLOCK_SIZE);
        if (db.size > 0) {
            size_t bytes_copied = 0;
            while (bytes_copied < db.size) {
                r = new record_key_count();
                assert(bytes_copied + sizeof(msg_key_int) <= db.size);
                m = *((msg_key_int *) db.data + bytes_copied / sizeof(msg_key_int));
                r->k = m.k;
                r->src = src;
                r->count = m.content;
                r->table_type = (tag == 0 ? 'R' : 'S');

                h_table->add(r);

                bytes_copied += sizeof(msg_key_int);
            }
        } else {
            t++;
        }
        CL->recv_end(db, src, tag);
    }

    return NULL;
}

static int M_B_(int *R, int *S, int hosts, int self, bool *S_migr) {
    int R_all = 0, R_local = 0, R_nodes = 0, S_nodes = 0, cost, max = 0, max_s;

    for (int i = 0; i < hosts; i++) {
        R_all += R[i];
        if (S[i] > 0) {
            R_local += R[i];
            S_nodes++;
            if (R[i] + S[i] > max) {
                max = R[i] + S[i];
                max_s = i;
            }
        }
        if (R[i] > 0 && i != self) {
            R_nodes++;
        }
        cost = R_all * S_nodes - R_local + R_nodes * S_nodes * sizeof(msg_key_int);
    }

    for (int i = 0; i < hosts; i++) {
        if (S[i] > 0 && i != max_s) {
            int delta = R[i] + S[i] - R_all - R_nodes * sizeof(msg_key_int);
            if (i != self) {
                delta += sizeof(msg_key_int);
            }
            if (delta < 0) {
                cost += delta;
                S_migr[i] = true;
            }
        }
    }

    return cost;
}

static void *notify_migration(void *param) {
    //table_r here in worker_param can be any type of table. It has no use in this function.
    worker_param<table_r, record_key_count> *p = (worker_param<table_r, record_key_count> *) param;
    ConnectionLayer *CL = p->CL;
    int tag = p->tag;
    HashTable<record_key_count> *h_table = p->h;

    size_t table_size = h_table->getSize();
    // Create hash table R, S
    //HashTable<record_count> R_C = new HashTable<record_count>(table_size);
    //HashTable<record_count> S_C = new HashTable<record_count>(table_size);

    int hosts = CL->get_hosts();
    int local_host = CL->get_local_host();

    DataBlock *dbs = new DataBlock[hosts];
    for (int dest = 0; dest < hosts; dest++) {
        while (!CL->send_begin(&dbs[dest], dest, tag));
        dbs[dest].size = 0;
    }

    record_key_count *r = NULL, *s = NULL;
    size_t k_index = -1;
	join_key_t k;
    msg_key_int m;

    // dimension 0: count; dimension 1: index in the hash table
    // store index here to help update hash table after migration
    int **R = new int *[2];
    int **S = new int *[2];

    for (int i = 0; i < 2; i++) {
        R[i] = new int[hosts];
        S[i] = new int[hosts];
    }

    // for all distinct key k in hash table
    while ((k_index = h_table->getNextKey(k_index + 1, k, true)) != table_size) {
        // initialize R and S to empty set
        for (int i = 0; i < hosts; i++) {
            R[0][i] = S[0][i] = 0;
        }
        size_t r_index = k_index - 1;
        while ((r_index = h_table->markVisited(k, r_index + 1, 'R', &r, true)) != table_size) {
            R[0][r->src] += r->count * sizeof(record_r);
            // for a specific key, one node only sends once to current node for R/S each
            R[1][r->src] = r_index;
        }
        size_t s_index = k_index - 1;
        while ((s_index = h_table->markVisited(k, s_index + 1, 'S', &s, true)) != table_size) {
            S[0][s->src] += s->count * sizeof(record_s);
            // for a specific key, one node only sends once to current node for R/S each
            S[1][s->src] = s_index;
        }

        bool *S_migr = new bool[hosts];
        bool *R_migr = new bool[hosts];
        // migrate & broadcast
        int RS_cost = M_B_(R[0], S[0], hosts, local_host, S_migr);
        int SR_cost = M_B_(S[0], R[0], hosts, local_host, R_migr);

        m.k = k;
        if (RS_cost < SR_cost) {
            int ds = 0;
            for (int ns = 0; ns < hosts; ns++) {
                if (S_migr[ns]) {
                    while (S_migr[ds]) {
                        // round robin to choose the migration destination
                        // Use round robin instead of one specific node to amortize the join workload to all nodes
                        ds = (ds + 1) % hosts;
                    }
                    // send <k, ds> to ns
                    m.content = ds;
                    if (dbs[ns].size + sizeof(msg_key_int) > BLOCK_SIZE) {
                        CL->send_end(dbs[ns], ns, tag);
                        while (!CL->send_begin(&dbs[ns], ns, tag));
                        dbs[ns].size = 0;
                    }
                    *((msg_key_int *) dbs[ns].data + dbs[ns].size / sizeof(msg_key_int)) = m;
                    dbs[ns].size += sizeof(msg_key_int);
                    assert(dbs[ns].size <= BLOCK_SIZE);
                    // update hash table
                    // change <k, ns> count to 0
                    h_table->del(S[1][ns]);
                    // change <k, ds> count to <k, ns> count + <k, ds> count
                    r->k = k;
                    r->count = (S[0][ns] + S[0][ds]) / sizeof(record_s);
                    r->src = ds;
                    r->table_type = 'S';
                    r->visited = true;
                    h_table->update(r, S[1][ds]);
                }
            }
        } else {
            int dr = 0;
            for (int nr = 0; nr < hosts; nr++) {
                if (R_migr[nr]) {
                    while (R_migr[dr]) {
                        // round robin to choose the migration destination
                        // Use round robin instead of one specific node to amortize the join workload to all nodes
                        dr = (dr + 1) % hosts;
                    }
                    // send <k, dr> to nr
                    m.content = dr;
                    if (dbs[nr].size + sizeof(msg_key_int) > BLOCK_SIZE) {
                        CL->send_end(dbs[nr], nr, tag);
                        while (!CL->send_begin(&dbs[nr], nr, tag));
                        dbs[nr].size = 0;
                    }
                    *((msg_key_int *) dbs[nr].data + dbs[nr].size / sizeof(msg_key_int)) = m;
                    dbs[nr].size += sizeof(msg_key_int);
                    assert(dbs[nr].size <= BLOCK_SIZE);
                    // update hash table
                    // change <k, nr> count to 0
                    h_table->del(R[1][nr]);
                    // change <k, dr> count to <k, nr> count + <k, dr> count
                    r->k = k;
                    r->count = (R[0][nr] + R[0][dr]) / sizeof(record_r);
                    r->src = dr;
                    r->table_type = 'R';
                    r->visited = true;
                    h_table->update(r, R[1][dr]);
                }
            }
        }

        delete S_migr;
        delete R_migr;
    }

    // Send last partially filled data blocks and end flags to all nodes
    for (int dest = 0; dest < hosts; dest++) {
        if (dbs[dest].size == 0) {
            CL->send_end(dbs[dest], dest, tag);
        } else {
            // Send last data blocks
            assert(dbs[dest].size <= BLOCK_SIZE);
            CL->send_end(dbs[dest], dest, tag);
            // Send end flags
            while (!CL->send_begin(&dbs[dest], dest, tag));
            dbs[dest].size = 0;
            CL->send_end(dbs[dest], dest, tag);
        }
    }

    return NULL;
}

// R or S
// recv notification : tag 2
// migrate tuples: tag 3
template<typename Table, typename Record>
static void *migrate_send(void *param) {
    worker_param<Table, Record> *p = (worker_param<Table, Record> *) param;
    ConnectionLayer *CL = p->CL;
    int tag = p->tag; // 3
    HashTable<Record> *h_table = p->h;

    int hosts = CL->get_hosts();
    //int local_host = CL->get_local_host();

    int src;
    DataBlock db_recv;
    DataBlock *db_send = new DataBlock[hosts];

    int dest;
    for (dest = 0; dest < hosts; dest++) {
        while (!CL->send_begin(&db_send[dest], dest, tag));
        db_send[dest].size = 0;
    }

    msg_key_int m;
    Record *r = NULL;

    int t = 0;
    while (t != hosts) {
        //receive notification from process T
        while (!CL->recv_begin(&db_recv, &src, 2));
        assert(db_recv.size <= BLOCK_SIZE);
        if (db_recv.size > 0) {
            size_t bytes_copied = 0;
            while (bytes_copied < db_recv.size) {
                m = *((msg_key_int *) db_recv.data + bytes_copied / sizeof(msg_key_int));
                bytes_copied += sizeof(msg_key_int);
                join_key_t k = m.k;
                dest = m.content;
                size_t pos = h_table->getSize();
                while ((pos = h_table->find(k, &r, pos + 1, 10)) != h_table->getSize()) {
                    if (db_send[dest].size + sizeof(Record) > BLOCK_SIZE) {
                        CL->send_end(db_send[dest], dest, tag);
                        while (!CL->send_begin(&db_send[dest], dest, tag));
                        db_send[dest].size = 0;
                    }
                    // send <k, payload> to destination node
                    *((Record *) db_send[dest].data + db_send[dest].size / sizeof(Record)) = *r;
                    db_send[dest].size += sizeof(Record);
                    assert(db_send[dest].size <= BLOCK_SIZE);
                }
            }
        } else {
            t++;
        }
        CL->recv_end(db_recv, src, 2);
    }

    // Send last partially filled data blocks and end flags to all nodes
    for (dest = 0; dest < hosts; dest++) {
        if (db_send[dest].size == 0) {
            CL->send_end(db_send[dest], dest, tag);
        } else {
            // Send last data blocks
            assert(db_send[dest].size <= BLOCK_SIZE);
            CL->send_end(db_send[dest], dest, tag);
            // Send end flags
            while (!CL->send_begin(&db_send[dest], dest, tag));
            db_send[dest].size = 0;
            CL->send_end(db_send[dest], dest, tag);
        }
    }

    return NULL;
}

// R or S
template<typename Table, typename Record>
static void *migrate_recv(void *param) {
    worker_param<Table, Record> *p = (worker_param<Table, Record> *) param;
    ConnectionLayer *CL = p->CL;
    int tag = p->tag;
    HashTable<Record> *h_table = p->h;

    int hosts = CL->get_hosts();
    //int local_host = CL->get_local_host();

    int src;
    DataBlock db;
    Record *r = NULL;

    int t = 0;
    while (t != hosts) {
        while (!CL->recv_begin(&db, &src, tag));
        assert(db.size <= BLOCK_SIZE);
        if (db.size > 0) {
            size_t bytes_copied = 0;
            while (bytes_copied < db.size) {
                r = new Record();
                assert(bytes_copied + sizeof(Record) <= db.size);
                bytes_copied += sizeof(Record);
                *r = *((Record *) db.data + bytes_copied / sizeof(Record));
                h_table->add(r);
            }
        } else {
            t++;
        }
    }

    return NULL;
}

// compute cost to broadcast R to S
static int B_(int *R, int *S, int hosts, int self) {
    int R_all = 0, R_local = 0, R_nodes = 0, S_nodes = 0, cost;

    for (int i = 0; i < hosts; i++) {
        R_all += R[i];
        if (S[i] > 0) {
            R_local += R[i];
            S_nodes++;
        }
        if (R[i] > 0 && i != self) {
            R_nodes++;
        }
        cost = R_all * S_nodes - R_local + R_nodes * S_nodes * sizeof(msg_key_int);
    }

    return cost;
}

static void *notify_broadcast(void *param) {
    //table_r here in worker_param can be any type of table. It has no use in this function.
    worker_param<table_r, record_key_count> *p = (worker_param<table_r, record_key_count> *) param;
    ConnectionLayer *CL = p->CL;
    int tag = p->tag;
    HashTable<record_key_count> *h_table = p->h;

    int hosts = CL->get_hosts();
    int local_host = CL->get_local_host();

    DataBlock *dbs = new DataBlock[hosts];
    int dest;
    for (dest = 0; dest < hosts; dest++) {
        while (!CL->send_begin(&dbs[dest], dest, tag));
        dbs[dest].size = 0;
    }

    size_t table_size = h_table->getSize();
    size_t k_index = -1;
    join_key_t k;
    msg_key_int m;

    record_key_count *r = NULL, *s = NULL;

    int *R = new int[hosts];
    int *S = new int[hosts];

    // for all distinct key k in hash table
    while ((k_index = h_table->getNextKey(k_index + 1, k, false) != table_size)) {
        for (int i = 0; i < hosts; i++) {
            R[i] = S[i] = 0;
        }
        size_t r_index = k_index - 1;
        while ((r_index = h_table->markVisited(k, r_index + 1, 'R', &r, false)) != table_size) {
            R[r->src] += r->count * sizeof(record_r);
            // for a specific key, one node only sends once to current node for R/S each
            //R[1][r->src] = r_index;
        }
        size_t s_index = k_index - 1;
        while ((s_index = h_table->markVisited(k, s_index + 1, 'S', &s, false)) != table_size) {
            S[s->src] += s->count * sizeof(record_s);
            // for a specific key, one node only sends once to current node for R/S each
            //S[1][s->src] = s_index;
        }

        // broadcast
        int RS_cost = B_(R, S, hosts, local_host);
        int SR_cost = B_(S, R, hosts, local_host);

        if (RS_cost < SR_cost) {
            for (int node_r = 0; node_r < hosts; node_r++) {
                if (R[node_r] != 0) {
                    for (int node_s = 0; node_s < hosts; node_s++) {
                        if (S[node_s] != 0) {
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
                }
            }
        } else {
            for (int node_s = 0; node_s < hosts; node_s++) {
                if (S[node_s] != 0) {
                    for (int node_r = 0; node_r < hosts; node_r++) {
                        if (R[node_r] != 0) {
                            m.k = k;
                            m.content = node_r;
                            if (dbs[node_s].size + sizeof(msg_key_int) > BLOCK_SIZE) {
                                CL->send_end(dbs[node_s], node_s, tag);
                                while (!CL->send_begin(&dbs[node_s], node_s, tag));
                                dbs[node_s].size = 0;
                            }
                            *((msg_key_int *) dbs[node_s].data + dbs[node_s].size / sizeof(msg_key_int)) = m;
                            dbs[node_s].size += sizeof(msg_key_int);
                            assert(dbs[node_s].size <= BLOCK_SIZE);
                        }
                    }
                }
            }
        }
    }

    // Send last partially filled data blocks and end flags to all nodes
    for (dest = 0; dest < hosts; dest++) {
        // Send last data blocks
        if (dbs[dest].size > 0) {
            assert(dbs[dest].size <= BLOCK_SIZE);
            CL->send_end(dbs[dest], dest, tag);
        }
        // Send end flags
        while (!CL->send_begin(&dbs[dest], dest, tag));
        dbs[dest].size = 0;
        CL->send_end(dbs[dest], dest, tag);
    }

    return NULL;
}

template<typename Table, typename Record>
static void *send_tuple(void *param) {
    worker_param<Table, Record> *p = (worker_param<Table, Record> *) param;
    ConnectionLayer *CL = p->CL;
    int tag = p->tag;
    HashTable<Record> *h_table = p->h;

    int hosts = CL->get_hosts();
    //int local_host = CL->get_local_host();

    int src;
    DataBlock db_recv;
    msg_key_int m;
    DataBlock *db_send = new DataBlock[hosts];

    int dest;
    for (dest = 0; dest < hosts; dest++) {
        while (!CL->send_begin(&db_send[dest], dest, tag));
        db_send[dest].size = 0;
    }

    Record *r;

    int t = 0;
    while (t != hosts) {
        while (!CL->recv_begin(&db_recv, &src, tag));
        assert(db_recv.size <= BLOCK_SIZE);
        if (db_recv.size > 0) {
            size_t bytes_copied = 0;
            while (bytes_copied < db_recv.size) {
                m = *((msg_key_int *) db_recv.data + bytes_copied / sizeof(msg_key_int));
                bytes_copied += sizeof(msg_key_int);
                join_key_t k = m.k;
                dest = m.content;

                r = new Record();
                size_t r_index = h_table->getSize();
                while ((r_index = h_table->find(k, &r, r_index + 1, 10)) != h_table->getSize()) {
                    //printf("Node %d send_tuple: r_index %lu, key %u, payload %u to Node %d\n", local_host, r_index, r->k, r->p, dest);
                    //fflush(stdout);
                    if (db_send[dest].size + sizeof(record_r) > BLOCK_SIZE) {
                        CL->send_end(db_send[dest], dest, tag);
                        while (!CL->send_begin(&db_send[dest], dest, tag));
                        db_send[dest].size = 0;
                    }
                    *((Record *) db_send[dest].data + db_send[dest].size / sizeof(Record)) = *r;
                    db_send[dest].size += sizeof(Record);
                    assert(db_send[dest].size <= BLOCK_SIZE);
                }
            }
        } else {
            t++;
        }
        CL->recv_end(db_recv, src, tag);
    }

    // Send last partially filled data blocks and end flags to all nodes
    for (dest = 0; dest < hosts; dest++) {
        // Send last data blocks
        if (db_send[dest].size > 0) {
            assert(db_send[dest].size <= BLOCK_SIZE);
            CL->send_end(db_send[dest], dest, tag);
        }
        // Send end flags
        while (!CL->send_begin(&db_send[dest], dest, tag));
        db_send[dest].size = 0;
        CL->send_end(db_send[dest], dest, tag);
    }

    return NULL;
}

// R or S
template<typename Table, typename Record>
static void *join_tuple(void *param) {
    worker_param<Table, Record> *p = (worker_param<Table, Record> *) param;
    ConnectionLayer *CL = p->CL;
    int tag = p->tag;
    HashTable<Record> *h_table = p->h;

    int hosts = CL->get_hosts();
    int local_host = CL->get_local_host();

    int src;
    DataBlock db_recv;

    Record *r1;
    Record *r2 = NULL;

    int t = 0;
    while (t != hosts) {
        while (!CL->recv_begin(&db_recv, &src, tag));
        assert(db_recv.size <= BLOCK_SIZE);

        if (db_recv.size > 0) {
            size_t bytes_copied = 0;
            while (bytes_copied < db_recv.size) {
                r1 = new Record();
                assert(bytes_copied + sizeof(Record) <= db_recv.size);
                *r1 = *((Record *) db_recv.data + bytes_copied / sizeof(Record));
                size_t pos = h_table->getSize();

                while ((pos = h_table->find(r1->k, &r2, pos + 1, 10)) != h_table->getSize()) {
                    bool valid = true;
                    printf("Join Result: Node %d #%lu, join_key %u payload_r1 %u, payload_r2 %u %s\n", local_host, trackjoin4_num,
                            r2->k, r1->p, r2->p, valid ? "correct" : "incorrect");
                    fflush(stdout);
                    trackjoin4_num++;
                }
                bytes_copied += sizeof(Record);
            }
        } else {
            t++;
        }
        CL->recv_end(db_recv, src, tag);
    }

    return NULL;
}


int TrackJoin4::run(ConnectionLayer *CL, struct table_r *R, struct table_s *S) {
    int t;
    worker_threads = new pthread_t[16];
    int local_host = CL->get_local_host();

    size_t h_table_r_size = R->num_records / 0.5;
    size_t h_table_s_size = S->num_records / 0.5;
    size_t h_table_key_size = (R->num_records + S->num_records) / 0.5;

    printf("Node %d, R hash table size = %lu, S hash table size = %lu\n", local_host, h_table_r_size, h_table_s_size);
    fflush(stdout);

//    HashTable<record_r> *h_table_r = new HashTable<record_r>(h_table_r_size);
//    HashTable<record_s> *h_table_s = new HashTable<record_s>(h_table_s_size);
//    HashTable<record_key_count> *h_table_key = new HashTable<record_key_count>(h_table_key_size);
//
//    worker_param<table_r, record_r> *param_r = new worker_param<table_r, record_r>();
//    param_r->CL = CL;
//    param_r->t = R;
//    param_r->tag = 0;
//    param_r->h = h_table_r;
//    pthread_create(&worker_threads[0], NULL, &scan_and_send<table_r, record_r>, (void *) param_r);
//
//    worker_param<table_s, record_s> *param_s = new worker_param<table_s, record_s>();
//    param_s->CL = CL;
//    param_s->t = S;
//    param_s->tag = 1;
//    param_s->h = h_table_s;
//    pthread_create(&worker_threads[1], NULL, &scan_and_send<table_s, record_s>, (void *) param_s);
//
//    // start 2 process T recv_key_count to receive keys sent from R and S respectively
//    for (t = 0; t < 2; t++) {
//        worker_param<table_r, record_key_count> *param_t = new worker_param<table_r, record_key_count>();
//        param_t->CL = CL;
//        param_t->tag = t;
//        param_t->h = h_table_key;
//        pthread_create(&worker_threads[t + 2], NULL, &recv_key_count, (void *) param_t);
//    }
//
//    // barrier
//    for (t = 0; t < 4; t++) {
//        void *retval;
//        pthread_join(worker_threads[t], &retval);
//    }
//
//    // notify migration
//    worker_param<table_r, record_key_count> *param_n = new worker_param<table_r, record_key_count>();
//    param_n->CL = CL;
//    param_n->tag = 2;
//    param_n->h = h_table_key;
//    pthread_create(&worker_threads[0], NULL, &notify_migration, (void *) param_n);

    // R, migrate send
//    param_r = new worker_param<table_r, record_r>();
//    param_r->CL = CL;
//    param_r->t = R;
//    param_r->tag = 3;
//    param_r->h = h_table_r;
//    pthread_create(&worker_threads[1], NULL, &migrate_send, (void *) param_r);
//
//    // S, migrate send
//    param_s = new worker_param<table_s, record_s>();
//    param_s->CL = CL;
//    param_s->t = S;
//    param_s->tag = 3;
//    pthread_create(&worker_threads[2], NULL, &migrate_send, (void *) param_s);
//
//    // R, migrate recv
//    param_r = new worker_param<table_r, record_r>();
//    param_r->CL = CL;
//    param_r->t = R;
//    param_r->tag = 3;
//    param_r->h = h_table_r;
//    pthread_create(&worker_threads[3], NULL, &migrate_recv, (void *) param_r);
//
//    // S, migrate recv
//    param_s = new worker_param<table_s, record_s>();
//    param_s->CL = CL;
//    param_s->tag = 3;
//    param_s->h = h_table_s;
//    pthread_create(&worker_threads[4], NULL, &migrate_recv, (void *) param_s);
//
//    for (t = 0; t < 5; t++) {
//        void *retval;
//        pthread_join(worker_threads[t], &retval);
//    }
//
//    // notify broadcast
//    worker_param<table_r, record_key_count> *param_b = new worker_param<table_r, record_key_count>();
//    param_b->CL = CL;
//    param_b->tag = 4;
//    param_b->h = h_table_key;
//    pthread_create(&worker_threads[5], NULL, &notify_broadcast, (void *) param_b);

    // R send tuple
    //worker_param<table_r,
}
