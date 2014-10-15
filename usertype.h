#ifndef _USERTYPE_H_
#define _USERTYPE_H_

#include <pthread.h>
#include <unordered_map>

#define MAX_TAG 3
#define MAX_NODES 255
#define MAX_CORES 32

#define RECV 0
#define SEND 1

#define BLOCK_SIZE 4096
#define BUFFER_SIZE 4096        // Set BUFFER_SIZE equal to BLOCK_SIZE to read one block of data each time
#define MAX_BLOCKS_PER_LIST 10  // Number of blocks initially allocated to the free lists

using namespace std;

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

class List {
public:
    List() {
        head = new ListNode();
        tail = new ListNode();
        head->next = tail;
        tail->prev = head;
        head->db.size = -1;
        tail->db.size = -1;
        num = 0;
        pthread_mutex_init(&mutex, NULL);
    };

    pthread_mutex_t mutex;

    ListNode *removeHead();
    int addTail(ListNode *);
    size_t getNum() {return num;}


private:
    ListNode *head;
    ListNode *tail;
    size_t num;     // Number of elements on the list
};

struct HashList {
    HashList() {
        pthread_mutex_init(&mutex, NULL);
    }

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
    int node;       // either source or destination node (depending on direction)
    int tag;
    int conn_type;       //0: read; 1: write
    int conn;            //connection file descriptor
};


#endif
