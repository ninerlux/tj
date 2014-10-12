#ifndef _USERTYPE_H_
#define _USERTYPE_H_

#include <pthread.h>
#include <unordered_map>
#define MAX_TAG 3
#define MAX_NODES 255
#define MAX_CORES 32

#define BLOCK_SIZE 4096
#define BUFFER_SIZE 4096 //set BUFFER_SIZE equal to BLOCK_SIZE to read one block of data each time
#define AVAL_BLOCKS 10000 //total number of available blocks in BLOCKPTRS

using namespace std;

//struct DataNode {
//    DataNode() {
//        src = dst = tag = -1;
//        size = 0;
//        data = NULL;
//        next = NULL;
//        prev = NULL;
//    };
//
//    DataNode(size_t s) : size(s) {
//        src = dst = tag = -1;
//        data = (void *)malloc(size);
//        next = NULL;
//        prev = NULL;
//    }
//
//    int src;
//    int dst;
//    int tag;
//    size_t size;
//    void *data;
//    DataNode *next;
//    DataNode *prev;
//};
//
//typedef struct DataNode_Queue {
//    DataNode_Queue() {
//        head = tail = NULL;
//        size_remain = 0;
//        pthread_mutex_init(&mutex, NULL);
//        pthread_cond_init(&cond, NULL);
//        processing = false;
//    }
//
//    DataNode *head;
//    DataNode *tail;
//    size_t size_remain;
//    pthread_mutex_t mutex;
//    pthread_cond_t cond;
//    bool processing;
//    bool empty;
//}DN_Queue;


struct DataBlock {
    DataBlock() {
        data = NULL;
        size = 0;
    };

    void *data;		// pointer to the data block
    size_t size;	// only represents the filled up data.  <= BLOCK_SIZE
};

struct ListNode {
    ListNode() {
        db = DataBlock();
        prev = NULL;
        next = NULL;
    };

    DataBlock db;
    ListNode *prev;
    ListNode *next;
};

struct List {
    List() {
        head = new ListNode();
        head->next = tail;
        head->db.size = -1;
        tail = new ListNode();
        tail->prev = head;
        tail->db.size = -1;
        num = 0;
        pthread_mutex_init(&mutex, NULL);
    };

    ListNode *head;
    ListNode *tail;
    size_t num;     // Number of elements on the list
    pthread_mutex_t mutex;
};

struct HashList {
    //the hash list contains pointers to data blocks
    //HashList is the data structure of 'busy' list
    //It is used to record history of data blocks sent to worker thread to process
    //We only guarantee the void *data is not tampered when it is sent back
    //We do not guarantee data content is not modified
    //The pointer to ListNode needs to be stored in hash table's value to be recycled
    unordered_map<void*, ListNode*> list;
    size_t num;
    pthread_mutex_t mutex;
};

struct msg {
    void *data;
    int size;
    int tag;
    int node;       // either source or destination node (depending on direction)
};

//parameters to pass to each thread
struct thr_param {
    int host;
    int tag;
    int conn_type;       //0: read; 1: write
    int conn;
};


#endif
