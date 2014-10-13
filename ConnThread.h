#ifndef _CONN_THREAD_H_
#define _CONN_THREAD_H_

#include "usertype.h"

extern void error(const char *msg);

extern List ***free_list, ***full_list;
extern HashList ***busy_list;
extern int localhost;

using namespace std;

//------------------  functions called by worker threads ----------------------

// Called by a worker thread when it wants to process received blocks
// Returns 0 if no data available
// Returns 1 if data available for tag, and populates db and src to point to DataBlock and have value of src
int recv_begin(DataBlock *db, int *src, int node_nr, int tag);

// Called by a worker thread when it is done processing a received block
// and that block of memory can be reused for another
void recv_end(DataBlock db, int src, int tag);

// Called by a worker thread when it is about to start filling block to be sent and needs a place for the output
// Returns 0 if ‘free’ is empty
// Returns 1 if data block is available
int send_begin(DataBlock *db, int dest, int tag);

// Called by a worker thread when it is done filling a to-be-sent block
void send_end(DataBlock db, int dest, int tag);

//------------------- functions called by connection threads -------------------
void *readFromSocket(void *param);

void *writeToSocket(void *param);

#endif