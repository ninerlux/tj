#include "ConnThread.h"
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>

// Called by a worker thread when it wants to process received blocks
// Returns 0 if no data available
// Returns 1 if data available for tag, and populates db and src to point to DataBlock- and have value of src
int recv_begin(DataBlock *db, int *src, int node_nr, int tag) {
    size_t largest_full_list_size;
    int largest_full_list_index;

    while (true) {
        // Look at all the 'full' receive lists for given tag (N of them)
        // and pick the one with highest 'num' (i.e. longest list).
        // Do this without locking
        largest_full_list_size = 0;
        largest_full_list_index = 0;
        if (full_list[tag] != NULL && full_list[tag][RECV] != NULL) {
            for (int h = 0; h < node_nr; h++) {
                if (full_list[tag][RECV][h].getNum() > largest_full_list_size) {
                    largest_full_list_size = full_list[tag][RECV][h].getNum();
                    largest_full_list_index = h;
                }
            }
        } else {
            error("recv_begin: full_list[tag] or full[tag][RECV] is NULL!");
        }

        if (largest_full_list_size == 0) {
            return 0;
        }

        // Lock that 'full' receive list
        pthread_mutex_lock(&full_list[tag][RECV][largest_full_list_index].mutex);

        // Check if largest_full_list_size > 0
        // Necessary because we checked the num before locking
        if (largest_full_list_size > 0) {
            break;
        } else {
            pthread_mutex_unlock(&full_list[tag][RECV][largest_full_list_index].mutex);
        }
    }

    // 'full' receive list is locked at this point
    // Pull the 1st node from the 'full' list
    ListNode *node = full_list[tag][RECV][largest_full_list_index].removeHead();
    if (node == NULL) {
        printf("recv_begin: pull node from list fail: tag %d, src %d\n", tag, largest_full_list_index);
        exit(-1);
    }
    pthread_mutex_unlock(&full_list[tag][RECV][largest_full_list_index].mutex);

    // Lock the corresponding 'busy' receive list (the one for the same src and tag)
    pthread_mutex_lock(&busy_list[tag][RECV][largest_full_list_index].mutex);

    // Move the node to the busy receive list
    *db = node->db;
    busy_list[tag][RECV][largest_full_list_index].list[db->data] = node;

    // Unlock the lists
    pthread_mutex_unlock(&busy_list[tag][RECV][largest_full_list_index].mutex);

    *src = largest_full_list_index;

    return 1;
}

// Called by a worker thread when it is done processing a received block
// and that block of memory can be reused for another
void recv_end(DataBlock db, int src, int tag) {
    ListNode *node = NULL;

    // Lock the 'busy' receive list for the given src and tag
    pthread_mutex_lock(&busy_list[tag][RECV][src].mutex);

    // Find the list node for that db on the 'busy' list (based on the value of the data pointer)
    unordered_map<void*, ListNode*>::const_iterator iter = busy_list[tag][RECV][src].list.find(db.data);
    if (iter != busy_list[tag][RECV][src].list.end()) {
        node = iter->second;
        busy_list[tag][RECV][src].list.erase(db.data);
    } else {
        pthread_mutex_unlock(&busy_list[tag][RECV][src].mutex);
        error("recv_end: data pointer tampered!");
    }

    // Unlock the 'busy' receive list
    pthread_mutex_unlock(&busy_list[tag][RECV][src].mutex);

    // Lock the 'free' receive list for the given src and tag
    pthread_mutex_lock(&free_list[tag][RECV][src].mutex);

    // Add that list node to the tail of the 'free' receive list
    if (free_list[tag][RECV][src].addTail(node) == -1) {
        printf("recv_end: add node to list fail: tag %d, src %d\n", tag, src);
        exit(-1);
    }

    // Unlock the 'free' receive list for the given src and tag
    pthread_mutex_unlock(&free_list[tag][RECV][src].mutex);
}


// Called by a worker thread when it is about to start working and needs a place for the output
// Returns 0 if 'free' is empty (checked without locking)
// Returns 1 if data block is available
int send_begin(DataBlock *db, int dest, int tag) {
    if (free_list[tag][SEND][dest].getNum() == 0) {
        return 0;
    }

    // Lock 'free' send list for the given dest and tag
    pthread_mutex_lock(&free_list[tag][SEND][dest].mutex);

    // Check size again
    if (free_list[tag][SEND][dest].getNum() == 0) {
        pthread_mutex_unlock(&free_list[tag][SEND][dest].mutex);
        return 0;
    }

    // Pull the 1st list node from the free send list
    ListNode *node = free_list[tag][SEND][dest].removeHead();
    if (node == NULL) {
        printf("send_begin: pull node from list fail: tag %d, dest %d\n", tag, dest);
        exit(-1);
    }

    // Unlock the 'free' send list
    pthread_mutex_unlock(&free_list[tag][SEND][dest].mutex);

    // Lock the corresponding 'busy' send list (the one for the same dest and tag)
    pthread_mutex_lock(&busy_list[tag][SEND][dest].mutex);

    // Move the node to the busy send list
    *db = node->db;
    busy_list[tag][SEND][dest].list[db->data] = node;

    // Unlock the 'busy' send list
    pthread_mutex_unlock(&busy_list[tag][SEND][dest].mutex);

    return 1;
}

// Called by a worker thread when it is done filling a to-be-sent block
void send_end(DataBlock db, int dest, int tag) {
    ListNode *node = NULL;

    // Lock the 'busy' send list for the given dest and tag
    pthread_mutex_lock(&busy_list[tag][SEND][dest].mutex);

    // Find the list node for that db on the 'busy' send list (based on the value of the data pointer)
    unordered_map<void*, ListNode*>::const_iterator iter = busy_list[tag][SEND][dest].list.find(db.data);
    if (iter != busy_list[tag][SEND][dest].list.end()) {
        node = iter->second;
        // Remove that list node from the 'busy' send list
        busy_list[tag][SEND][dest].list.erase(db.data);
    } else {
        pthread_mutex_unlock(&busy_list[tag][SEND][dest].mutex);
        error("send_end: data pointer tampered!");
    }

    // Unlock the 'busy' send list
    pthread_mutex_unlock(&busy_list[tag][SEND][dest].mutex);

    // Copy data block into node (by value, since its just a pointer and size_t)
    node->db = db;

    // Lock the 'full' send list for the given dest and tag
    pthread_mutex_lock(&full_list[tag][SEND][dest].mutex);

    // Add that list node to the tail of the 'full' send list
    if (full_list[tag][SEND][dest].addTail(node) == -1) {
        printf("send_end: add node to list fail: tag %d, dest %d\n", tag, dest);
        exit(-1);
    }

    // Unlock the 'full' send list for the given dest and tag
    pthread_mutex_unlock(&full_list[tag][SEND][dest].mutex);
}

// Called by the read connection thread
// Can block on read
void *readFromSocket(void *param) {
    thr_param *p = (thr_param *) param;
    ListNode *node;
    int src = p->node;
    int tag = p->tag;
    int conn_fd = p->conn;
    bool pull_new_free_node = true;
    size_t space_remain_in_cur_node = 0;

    while (true) {
        if (src == local_host) {
            // Local data transfer.  Coud optimize by acquiring both locks and then transferring everything, not just one node
            // Pull the 1st node from the 'full' send list
            pthread_mutex_lock(&full_list[tag][SEND][local_host].mutex);
            node = full_list[tag][SEND][local_host].removeHead();
            pthread_mutex_unlock(&full_list[tag][SEND][local_host].mutex);

            if (node == NULL) {
                continue;
            }
       
            // Add the node to the tail of the 'full' receive list
            pthread_mutex_lock(&full_list[tag][RECV][local_host].mutex);
            if (full_list[tag][RECV][local_host].addTail(node) == -1) {
                printf("readFromSocket: add node to list fail: tag %d, local\n", tag);
                pthread_exit(NULL);
            }
            pthread_mutex_unlock(&full_list[tag][RECV][local_host].mutex);
        } else {
            if (pull_new_free_node) {
                // Pull the 1st list node from the 'free' receive list
                pthread_mutex_lock(&free_list[tag][RECV][src].mutex);
                node = free_list[tag][RECV][src].removeHead();
                pthread_mutex_unlock(&free_list[tag][RECV][src].mutex);

                if (node == NULL) {
                    continue;
                }
                space_remain_in_cur_node = BLOCK_SIZE;
            }

            // Read from socket() in that list node
            ///printf("Node %d going to read from node %d for tag %d\n", local_host, src, tag);
            //fflush(stdout);
            size_t n;
            n = read(conn_fd, node->db.data, space_remain_in_cur_node);
            //printf("Node %d read >%s< with size %ld from node %d for tag %d\n", local_host, (char *) node->db.data, n, src, tag);
            //fflush(stdout);
            if (n == 0) {
               continue;
            }

            if (n < 0) {
                printf("readFromSocket: error on reading from src %d on tag %d\n", src, tag);
                pthread_exit(NULL);
            }

            space_remain_in_cur_node -= n;
            node->db.size += n;

            if (node->db.size > BLOCK_SIZE) {
                printf("readFromSocket: error on datablock size %lu from src %d on tag %d\n",
                        node->db.size, src, tag);
                pthread_exit(NULL);
            }

            if (space_remain_in_cur_node == 0)
                pull_new_free_node = true;

            if (pull_new_free_node) { // Current node has been fully filled, next time we need to pull new node
                // Add the list node to the tail of the 'full' receive list
                pthread_mutex_lock(&full_list[tag][RECV][src].mutex);
                if (full_list[tag][RECV][src].addTail(node) == -1) {
                    printf("readFromSocket: add node to list fail: tag %d, src %d\n", tag, src);
                    pthread_exit(NULL);
                }
                printf("Node %d added to tail of %d receive from %d for %ld bytes with %ld left\n", local_host, tag, src, n, space_remain_in_cur_node);
                fflush(stdout);
                pthread_mutex_unlock(&full_list[tag][RECV][src].mutex);
            }
        }
    }
}

// Called by the write connection thread
void *writeToSocket(void *param) {
    thr_param *p = (thr_param *)param;
    ListNode *node;
    int dest = p->node;
    int tag = p->tag;
    int conn_fd = p->conn;

    while (true) {
        if (dest == local_host) { 
            // Local data transfer
            // Not necesssary to do anything here, since readToSocket pulls from the 'full' send list
        } else {
            // Pull the 1st node from the 'full' send list
            pthread_mutex_lock(&full_list[tag][SEND][dest].mutex);
            node = full_list[tag][SEND][dest].removeHead();
            pthread_mutex_unlock(&full_list[tag][SEND][dest].mutex);

            if (node == NULL) {
                continue;
            }

            // Write data in that node to socket
            //printf("Node %d writing >%s< with size %ld to node %d\n", local_host, (char *) node->db.data, node->db.size, dest);
            //fflush(stdout);
            size_t n;
            n = write(conn_fd, node->db.data, node->db.size);
 
            if (n < 0) {
                printf("writeToSocket: error on writing to dest %d on tag %d\n", dest, tag);
                pthread_exit(NULL);
            }

            if (n != node->db.size) {
                printf("writeToSocket: error - partial writing to dest %d on tag %d with size %lu\n", dest, tag, n);
                pthread_exit(NULL);
            }

            // Add the node to the tail of the 'free' send list
            pthread_mutex_lock(&free_list[tag][SEND][dest].mutex);
            if (free_list[tag][SEND][dest].addTail(node) == -1) {
                printf("writeToSocket: add node to list fail: tag %d, dest %d\n", tag, dest);
                pthread_exit(NULL);
            }
            pthread_mutex_unlock(&free_list[tag][SEND][dest].mutex);
        }
    }
}
