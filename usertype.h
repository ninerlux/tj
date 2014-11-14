#ifndef _USERTYPE_H_
#define _USERTYPE_H_

#include <pthread.h>
#include <stdint.h>
#include <unordered_map>
#include <assert.h>

#define MAX_TAG 3
#define MAX_NODES 255
#define MAX_CORES 32

#define BLOCK_SIZE 4096
#define BUFFER_SIZE 4096 	//set BUFFER_SIZE equal to BLOCK_SIZE to read one block of data each time
#define MAX_BLOCKS_PER_LIST 100 // Number of blocks initially allocated to the free list

#define BYTES_PAYLOAD_R 4
#define BYTES_PAYLOAD_S 4

using namespace std;

typedef uint32_t join_key_t;
typedef struct r_payload_t { uint8_t bytes[BYTES_PAYLOAD_R]; } r_payload_t;
typedef struct s_payload_t { uint8_t bytes[BYTES_PAYLOAD_S]; } s_payload_t;

struct record_r {
    join_key_t k;
    r_payload_t p;
};

struct record_s {
    join_key_t k;
    s_payload_t p;
};

struct table_r {
    record_r *records;
	int num_bytes;
	int num_records;
};

struct table_s {
    record_s *records;
	int num_bytes;
	int num_records;
};

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

    void lock() { pthread_mutex_lock(&mutex); };
    void unlock() { pthread_mutex_unlock(&mutex); };

    ListNode *removeHead();
    int addTail(ListNode *);
    
    ListNode *removeHeadSafe();
    int addTailSafe(ListNode *);

    size_t getNum() {return num;}


private:
    ListNode *head;
    ListNode *tail;
    size_t num;     // Number of elements on the list
};

class HashList {
public:
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

    void lock() { pthread_mutex_lock(&mutex); };
    void unlock() { pthread_mutex_unlock(&mutex); };
};

class HashTable {
public:
    //local HashTable for hash join
    //The HashTable stores keys and payloads in table R
    HashTable(size_t size, float ld = 0.75) : num(size), load_factor(ld) {
        table = new record_r[num];
		for (int i = 0; i < num; i++) {
			assert(table[num].k == 0 && table[num].p.bytes[0] == 0 && table[num].p.bytes[1] == 0 && table[num].p.bytes[2] == 0 && table[num].p.bytes[3] == 0 );
		}
    };

    int hash(join_key_t k);
    int add(record_r r);
    int find(join_key_t k, record_r *r, int index, size_t nr_results);	//index: starting searching index
    int num;
    float load_factor;  // hash table need to consider load factor to grow
    record_r *table;
};

struct msg {
    void *data;
    int size;
    int tag;
    int node;		//either source of destionation node (depending on direction)
};

//parameters to pass to each thread
struct thr_param {
    int node;       	// either source or destination node (depending on direction)
    int tag;
    int conn_type;       //0: read; 1: write
    int conn;            //connection file descriptor
};

#endif
