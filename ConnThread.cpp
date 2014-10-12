#include "ConnThread.h"

// Called by a worker thread when it wants to process received blocks
// Returns 0 if no data available
// Returns 1 if data available for tag, and populates db and src to point to DataBlock- and have value of src
int recv_begin(DataBlock *db, int *src, int node_nr, int tag) {
    size_t largest_full_list_size;
    int largest_full_list_index;

    while (true) {
        //look at all the ‘full’ receive lists for given tag (N of them)
        // and pick the one with highest ‘num’ (i.e. longest list).
        // Do this without locking
        largest_full_list_size = 0;
        largest_full_list_index = 0;
        if (full_list[tag] != NULL && full_list[tag][0] != NULL) {
            for (int h = 0; h < node_nr; h++) {
                if (full_list[tag][0][h].num > largest_full_list_size) {
                    largest_full_list_size = full_list[tag][0][h].num;
                    largest_full_list_index = h;
                }
            }
        } else {
            error("recv_begin: full_list[tag] or full[tag][0] is NULL!");
        }

        if (largest_full_list_size == 0) {
            return 0;
        }

        //lock that ‘full’ receive list
        pthread_mutex_lock(&full_list[tag][0][largest_full_list_index].mutex);
        // check if largest_full_list_size > 0
        // necessary because we checked the num before locking
        if (largest_full_list_size > 0) {
            break;
        } else {
            pthread_mutex_unlock(&full_list[tag][0][largest_full_list_index].mutex);
        }
    }

    //'full' receive list is already locked now
    // get the 1st node on the 'full' list
    ListNode *head = full_list[tag][0][largest_full_list_index].head;
    ListNode *node = head->next;
    head->next = node->next;
    node->next->prev = head;
    node->prev = NULL;
    node->next = NULL;

    //lock the corresponding ‘busy’ receive list (the one for the same src and tag)
    pthread_mutex_lock(&busy_list[tag][0][largest_full_list_index].mutex);
    //move the node to the busy receive list
    *db = node->db;
    busy_list[tag][0][largest_full_list_index].list[db->data] = node;
    //unlock the ‘busy’ receive list
    pthread_mutex_unlock(&busy_list[tag][0][largest_full_list_index].mutex);

    //unlock the 'full' receive list
    pthread_mutex_unlock(&full_list[tag][0][largest_full_list_index].mutex);

    *src = largest_full_list_index;

    return 1;
}

// Called by a worker thread when it is done processing a received block
// and that block of memory can be reused for another
void recv_end(DataBlock db, int src, int tag) {
    ListNode *node = NULL;

    //lock the ‘busy’ receive list for the given src and tag
    pthread_mutex_lock(&busy_list[tag][0][src].mutex);
    //find the list node for that db on the ‘busy’ list (based on the value of the data pointer)
    unordered_map<void*, ListNode*>::const_iterator iter = busy_list[tag][0][src].list.find(db.data);
    if (iter != busy_list[tag][0][src].list.end()) {
        node = iter->second;
        busy_list[tag][0][src].list.erase(db.data);
    } else {
        pthread_mutex_unlock(&busy_list[tag][0][src].mutex);
        error("recv_end: data pointer tampered!");
    }
    //unlock the ‘busy’ receive list
    pthread_mutex_unlock(&busy_list[tag][0][src].mutex);

    //lock the ‘free’ receive list for the given src and tag
    pthread_mutex_lock(&free_list[tag][0][src].mutex);
    //add that list node to the ‘free’ receive list
    ListNode *tail = free_list[tag][0][src].tail;
    node->next = tail;
    node->prev = tail->prev;
    tail->prev = node;
    node->prev->next = node;
    //unlock the ‘free’ receive list for the given src and tag
    pthread_mutex_unlock(&free_list[tag][0][src].mutex);
}


// Called by a worker thread when it is about to start working and needs a place for the output
// Returns 0 if ‘free’ is empty
// Returns 1 if data block is available
int send_begin(DataBlock *db, int dest, int tag) {
    //lock ‘free’ send list for the given dest and tag
    pthread_mutex_lock(&free_list[tag][1][dest].mutex);
    if (free_list[tag][1][dest].num == 0) {
        pthread_mutex_unlock(&free_list[tag][1][dest].mutex);
        return 0;
    }

    //get the 1st list node on the free send list
    ListNode *head = free_list[tag][1][dest].head;
    ListNode *node = head->next;
    head->next = node->next;
    node->next->prev = head;
    node->prev = NULL;
    node->next = NULL;
    //unlock the 'free' send list
    pthread_mutex_unlock(&free_list[tag][1][dest].mutex);

    //lock the corresponding ‘busy’ send list (the one for the same dest and tag)
    pthread_mutex_lock(&busy_list[tag][1][dest].mutex);
    //move the node to the busy send list
    *db = node->db;
    busy_list[tag][1][dest].list[db->data] = node;
    //unlock the 'busy' send list
    pthread_mutex_unlock(&busy_list[tag][1][dest].mutex);

    return 1;
}

// Called by a worker thread when it is done filling a to-be-sent block
void send_end(DataBlock db, int dest, int tag) {
    ListNode *node = NULL;

    //lock the ‘busy’ send list for the given dest and tag
    pthread_mutex_lock(&busy_list[tag][1][dest].mutex);
    //find the list node for that db on the ‘busy’ send list (based on the value of the data pointer)
    unordered_map<void*, ListNode*>::const_iterator iter = busy_list[tag][1][dest].list.find(db.data);
    if (iter != busy_list[tag][1][dest].list.end()) {
        node = iter->second;
        //remove that list node from the ‘busy’ send list
        busy_list[tag][1][dest].list.erase(db.data);
    } else {
        pthread_mutex_unlock(&busy_list[tag][1][dest].mutex);
        error("send_end: data pointer tampered!");
    }
    //unlock the ‘busy’ send list
    pthread_mutex_unlock(&busy_list[tag][1][dest].mutex);

    //lock the ‘full’ send list for the given dest and tag
    pthread_mutex_lock(&full_list[tag][1][dest].mutex);
    //add that list node to the ‘full’ send list
    ListNode *tail = full_list[tag][1][dest].tail;
    node->next = tail;
    node->prev = tail->prev;
    tail->prev->next = node;
    tail->prev = node;
    //unlock the ‘full’ send list for the given dest and tag
    pthread_mutex_unlock(&full_list[tag][1][dest].mutex);
}