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
        if (h.find_s(T->records[i].k, &r, h.getNum(), 10) < 0) {
            dest = h.hash32(T->records[i].k) % hosts;
        }

    }
}
