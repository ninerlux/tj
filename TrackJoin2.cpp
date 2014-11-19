#include "Algorithms.h"
#include "ConnectionLayer.h"
#include "usertype.h"
#include "HashTable.h"
#include <string.h>
#include <assert.h>

template <typename Table, typename Record>
class worker_param {
public:
    int tag;
    ConnectionLayer *CL;
    Table *t;
    int start_index, end_index;
    HashTable<Record> *h;
};

int TrackJoin::get_tags() {
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
        if (h_table->find(T->records[i].k, &r, h_table->getNum(), 10) < 0) {
            h_table->add(&(T->records[i]));
            dest = h_table->hash32(T->records[i].k) % hosts;
            if (dbs[dest].size + sizeof(join_key_t) > BLOCK_SIZE) {
                CL->send_end(dbs[dest], dest, tag); //msgs with tag 0 are sent to process T
                //printf("Scan - Node %d send data block to node %d with %lu records\n", local_host, dest, dbs[dest].size / sizeof(Record));
                //fflush(stdout);
                while (!CL->send_begin(&dbs[dest], dest, tag));
                dbs[dest].size = 0;
            }
            *((Record *) dbs[dest].data + dbs[dest].size / sizeof(join_key_t)) = T->records[i].k;
            dbs[dest].size += sizeof(join_key_t);
            assert(dbs[dest].size <= BLOCK_SIZE);
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

static void *recv_keys(void *param) {
    //table_r here in worker_param can be any type of table. It has no use in this function.
    worker_param<table_r, record_node> *p = (worker_param<table_r, record_node> *) param;
    ConnectionLayer *CL = p->CL;
    int tag = p->tag;
    HashTable<record_node> *h_table = p->h;

    int hosts = CL->get_hosts();
    int local_host = CL->get_local_host();

    int src;
    record_node *r;
    DataBlock db;

    int t = 0;
    // Receive until termination received from all nodes
    while (t != hosts) {
        while (!CL->recv_begin(&db, &src, tag));
        assert(db.size <= BLOCK_SIZE);
        if (db.size > 0) {
            size_t bytes_copied = 0;
            while (bytes_copied < db.size) {
                r = new record_node();
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

static void *notify_R_nodes(void *param) {
    //table_r here in worker_param can be any type of table. It has no use in this function.
    worker_param<table_r, record_node> *p = (worker_param<table_r, record_node> *) param;
    ConnectionLayer *CL = p->CL;
    HashTable<record_node> *h_table = p->h;

    int hosts = CL->get_hosts();
    int local_host = CL->get_local_host();

    DataBlock *dbs = new DataBlock[hosts];
    // prepare data blocks for each destination
    int dest;
    for (dest = 0; dest < hosts; dest++) {
        while (!CL->send_begin(&dbs[dest], dest, 1));
        dbs[dest].size = 0;
    }

    size_t table_size = h_table->getSize();
    size_t k_index = 0;
    while ((k_index = h_table->getNextKey(k_index + 1, k)) != table_size) {
        size_t r_index = 0;
        int node_r = -1;
        while ((r_index = h_table->erase(k, r_index + 1, 'R', node_r, 10)) != table_size) {
            size_t s_index = 0;
            int node_s = -1;
            while ((s_index = h_table->erase(k, s_index + 1, 'S', node_s, 10)) != table_size) {
                // send <k, node_s> to node_r, use tag 0

            }
        }
    }
}

// R
static void *send_tuple(void *param) {
    worker_param<table_r, record_r> *p = (worker_param<table_r, record_r> *) param;
    ConnectionLayer *CL = p->CL;
    HashTable<record_r> *h_table = p->h;

    int hosts = CL->get_hosts();

    DataBlock db;

    int t = 0;
    while (t != hosts) {
        while (!CL->recv_begin(&db, &src, 0));
        assert(db.size <= BLOCK_SIZE);
        if (db.size > 0) {
            size_t bytes_copied = 0;
            while (bytes_copied < db.size) {

            }
        } else {
            t++;
        }
    }
}