#ifndef _USERTYPE_H_
#define _USERTYPE_H_

#define MAX_TAG 3
#define MAX_NODES 255
#define MAX_CORES 32

#define BLOCK_SIZE 4096
#define BUFFER_SIZE 4096 //set BUFFER_SIZE equal to BLOCK_SIZE to read one block of data each time
#define AVAL_BLOCKS 10000 //total number of available blocks in BLOCKPTRS

struct DataNode {
    DataNode() {
        src = dst = tag = -1;
        size = 0;
        data = NULL;
        next = NULL;
        prev = NULL;
    };

    DataNode(size_t s) : size(s) {
        src = dst = tag = -1;
        data = (void *)malloc(size);
        next = NULL;
        prev = NULL;
    }

    int src;
    int dst;
    int tag;
    size_t size;
    void *data;
    DataNode *next;
    DataNode *prev;
};

typedef struct DataNode_Queue {
    DataNode_Queue() {
        head = tail = NULL;
        size_remain = 0;
        pthread_mutex_init(&mutex, NULL);
        pthread_cond_init(&cond, NULL);
        processing = false;
    }

    DataNode *head;
    DataNode *tail;
    size_t size_remain;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    bool processing;
    bool empty;
}DN_Queue;

//parameters to pass to each thread
struct thr_param {
    int host;
    int tag;
    int conn;
};

#endif
