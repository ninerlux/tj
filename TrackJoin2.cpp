#include "Algorithms.h"
#include "ConnectionLayer.h"
#include "usertype.h"

template <typename Table, typename Record>
class worker_param {
public:
    int tag;
    ConnectionLayer *CL;
    Table *t;
    int start_index, end_index;
    HashTable<Record> *h;
};

template <typename Table, typename Record>
static void *scan_and_send(void *param) {
    worker_param<Table, Record> *p = (worker_param<Table, Record> *) param;
    ConnectionLayer *CL = p->CL;
    Table *T = p->t;

    HashTable<Record> h(T->num_records / 0.1);

    int hosts = CL->get_hosts();

    int dest;
    DataBlock *dbs = new DataBlock[hosts];
    // prepare data blocks for each destination
    for (dest = 0; dest < hosts; dest++) {
        while (!CL->send_begin(&dbs[dest], dest, 1));
        dbs[dest].size = 0;
    }

    Record *r = NULL;
    // Send each record in T to destination node
    for (int i = 0; i < T->num_records; i++) {
        // check if key is duplicate
        if (h.find(T->records[i].k, &r, h.getNum(), 10) < 0) {
            h.add(&(T->records[i]));
            dest = h.hash32(T->records[i].k) % hosts;
            if (dbs[dest].size + sizeof(Record) > BLOCK_SIZE) {
                CL->send_end(dbs[dest], dest, 0); //msgs with tag 0 are sent to process T
                //printf("Scan - Node %d send data block to node %d with %lu records\n", local_host, dest, dbs[dest].size / sizeof(Record));
                //fflush(stdout);
                while (!CL->send_begin(&dbs[dest], dest, 0));
                dbs[dest].size = 0;
            }
            *((Record *) dbs[dest].data + dbs[dest].size / sizeof(Record)) = T->records[i];
            dbs[dest].size += sizeof(Record);
            assert(dbs[dest].size <= BLOCK_SIZE);
        }
    }

    // Send last partially filled data blocks and end flags to all nodes
    for (dest = 0; dest < hosts; dest++) {
        // Send last data blocks
        if (dbs[dest].size > 0) {
            assert(dbs[dest].size <= BLOCK_SIZE);
            CL->send_end(dbs[dest], dest, 1);
            //printf("Scan - Node %d send data block to node %d with %lu records\n", local_host, dest, dbs[dest].size / sizeof(Record));
            //fflush(stdout);
        }
        // Send end flags
        while (!CL->send_begin(&dbs[dest], dest, 1));
        dbs[dest].size = 0;
        CL->send_end(dbs[dest], dest, 1);
        //printf("Scan - Node %d send end flag node %d with size %lu\n", local_host, dest, dbs[dest].size);
        //fflush(stdout);
    }

    return NULL;
}

static void *recv_and_build(void *param) {

}
