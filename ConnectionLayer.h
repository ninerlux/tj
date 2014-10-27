#ifndef _CONN_THREAD_H_
#define _CONN_THREAD_H_

#include "usertype.h"

#define MAX_HOSTS 255
#define NUM_CONN_TYPES 2
#define RECV 0
#define SEND 1

extern void error(const char *msg);

using namespace std;

class ConnectionLayer {
public:
    ConnectionLayer(const char *conf, const char *domain, int tags);

    int get_hosts();
    int get_local_host();

    // Called by a worker thread when it wants to process received blocks
    // Returns 0 if no data available
    // Returns 1 if data available for tag, and populates db and src to point to DataBlock and have value of src
    int recv_begin(DataBlock *db, int *src, int tag);

    // Called by a worker thread when it is done processing a received block
    // and that block of memory can be reused for another
    void recv_end(DataBlock db, int src, int tag);

    // Called by a worker thread when it is about to start filling block to be sent and needs a place for the output
    // Returns 0 if ‘free’ is empty
    // Returns 1 if data block is available
    int send_begin(DataBlock *db, int dest, int tag);

    // Called by a worker thread when it is done filling a to-be-sent block
    void send_end(DataBlock db, int dest, int tag);

private:
    int tags;
    int hosts;
    int local_host;
    int server;
    int **conn;
    pthread_t ***conn_threads;
    List ***free_list, ***full_list;
    ListStat *full_recv_list_stats;
    HashList ***busy_list;

    static void *doReadFromSocket(void *context);
    static void *doWriteToSocket(void *context);
    void *readFromSocket(void *param);
    void *writeToSocket(void *param);

    int **setupGrid(const char *conf, const char *domain);
    void createLists();
    void startThreads();
};

struct CLContext {
    ConnectionLayer *CL;
    thr_param *param;
};


#endif
