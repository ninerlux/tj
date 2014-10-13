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
        //look at all the ‘full’ receive lists for given tag (N of them)
        // and pick the one with highest ‘num’ (i.e. longest list).
        // Do this without locking
        largest_full_list_size = 0;
        largest_full_list_index = 0;
        if (full_list[tag] != NULL && full_list[tag][0] != NULL) {
            for (int h = 0; h < node_nr; h++) {
                if (full_list[tag][0][h].getNum() > largest_full_list_size) {
                    largest_full_list_size = full_list[tag][0][h].getNum();
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
    // pull the 1st node from the 'full' list
    ListNode *node = full_list[tag][0][largest_full_list_index].removeHead();
    if (node == NULL) {
        printf("recv_begin: pull node from list fail: tag %d, src %d\n", tag, largest_full_list_index);
        exit(-1);
    }

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
    //add that list node to the tail of the ‘free’ receive list
    if (free_list[tag][0][src].addTail(node) == -1) {
        printf("recv_end: add node to list fail: tag %d, src %d\n", tag, src);
        exit(-1);
    }

    //unlock the ‘free’ receive list for the given src and tag
    pthread_mutex_unlock(&free_list[tag][0][src].mutex);
}


// Called by a worker thread when it is about to start working and needs a place for the output
// Returns 0 if ‘free’ is empty
// Returns 1 if data block is available
int send_begin(DataBlock *db, int dest, int tag) {
    //lock ‘free’ send list for the given dest and tag
    pthread_mutex_lock(&free_list[tag][1][dest].mutex);
    if (free_list[tag][1][dest].getNum() == 0) {
        pthread_mutex_unlock(&free_list[tag][1][dest].mutex);
        return 0;
    }

    //pull the 1st list node from the free send list
    ListNode *node = free_list[tag][1][dest].removeHead();
    if (node == NULL) {
        printf("send_begin: pull node from list fail: tag %d, dest %d\n", tag, dest);
        exit(-1);
    }

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
    //add that list node to the tail of the ‘full’ send list
    if (full_list[tag][1][dest].addTail(node) == -1) {
        printf("send_end: add node to list fail: tag %d, dest %d\n", tag, dest);
        exit(-1);
    }
    //unlock the ‘full’ send list for the given dest and tag
    pthread_mutex_unlock(&full_list[tag][1][dest].mutex);
}

//function called by the read connection thread
//It can block on read
void *readFromSocket(void *param) {
    thr_param *p = (thr_param *)param;
    ListNode *node;
    int src = p->node;
    int tag = p->tag;
    int conn_fd = p->conn;
    bool pull_new_free_node = true;
    size_t space_remain_in_cur_node = 0;
    while (true) {
        if (src == local_host) {
            //local data transfer
            //lock the 'full' send list
            pthread_mutex_lock(&full_list[tag][1][local_host].mutex);
            //pull the 1st node from the 'full' send list
            node = full_list[tag][1][local_host].removeHead();
            if (node == NULL) {
                printf("readFromSocket: pull node from list fail: tag %d, local\n", tag);
                pthread_exit(NULL);
            }
            //unlock the 'full' send list
            pthread_mutex_unlock(&full_list[tag][1][local_host].mutex);

            //lock the 'full' receive list
            pthread_mutex_lock(&full_list[tag][0][local_host].mutex);
            //add the node to the tail of the 'full' receive list
            if (full_list[tag][0][local_host].addTail(node) == -1) {
                printf("readFromSocket: add node to list fail: tag %d, local\n", tag);
                pthread_exit(NULL);
            }
            //unlock the 'full' receive list
            pthread_mutex_unlock(&full_list[tag][0][local_host].mutex);

        } else {
            if (pull_new_free_node) {
                //lock the 'free' receive list
                pthread_mutex_lock(&free_list[tag][0][src].mutex);
                //pull the 1st list node from the 'free' receive list
                node = free_list[tag][0][src].removeHead();
                if (node == NULL) {
                    printf("readFromSocket: pull node from list fail: tag %d, src %d\n", tag, src);
                    pthread_exit(NULL);
                }
                space_remain_in_cur_node = BLOCK_SIZE;
                //unlock the 'free' receive list
                pthread_mutex_unlock(&free_list[tag][0][src].mutex);
            }
            //read from socket() in that list node
            size_t n;

            n = read(conn_fd, node->db.data, space_remain_in_cur_node);
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
            if (space_remain_in_cur_node == 0) {
                pull_new_free_node = true;
            }

            if (pull_new_free_node) { //Current node has been fully filled, next time we need to pull new node
                //lock the 'full' receive list
                pthread_mutex_lock(&full_list[tag][0][src].mutex);
                //add the list node to the tail of the 'full' receive list
                if (full_list[tag][0][src].addTail(node) == -1) {
                    printf("readFromSocket: add node to list fail: tag %d, src %d\n", tag, src);
                    pthread_exit(NULL);
                }
                //unlock the 'full' receive list
                pthread_mutex_unlock(&full_list[tag][0][src].mutex);
            }
        }
    }
}

//function called by the write connection thread
void *writeToSocket(void *param) {
    thr_param *p = (thr_param *)param;
    ListNode *node;
    int dest = p->node;
    int tag = p->tag;
    int conn_fd = p->conn;
    while (true) {
        if (dest == local_host) { //local data transfer
            //lock the 'full' receive list
            pthread_mutex_lock(&full_list[tag][0][local_host].mutex);
            //pull the 1st node from the ‘full’ receive list
            node = full_list[tag][0][local_host].removeHead();
            if (node == NULL) {
                printf("writeToSocket: pull node from list fail: tag %d, local\n", tag);
                pthread_exit(NULL);
            }
            //unlock ‘full’ receive list
            pthread_mutex_unlock(&full_list[tag][0][local_host].mutex);

            //lock the 'full' send list
            pthread_mutex_lock(&full_list[tag][1][local_host].mutex);
            //add the node to the tail of the 'full' send list
            if (full_list[tag][1][local_host].addTail(node) == -1) {
                printf("writeToSocket: add node to list fail: tag %d, local\n", tag);
                pthread_exit(NULL);
            }
            //unlock the 'full' send list
            pthread_mutex_unlock(&full_list[tag][1][local_host].mutex);
        } else {
            //lock the ‘full’ send list
            pthread_mutex_lock(&full_list[tag][1][dest].mutex);
            //pull the 1st node from the ‘full’ send list
            node = full_list[tag][1][dest].removeHead();
            if (node == NULL) {
                printf("writeToSocket: pull node from list fail: tag %d, dest %d\n", tag, dest);
                pthread_exit(NULL);
            }
            //unlock ‘full’ send list
            pthread_mutex_unlock(&full_list[tag][1][dest].mutex);

            //write data in that node to socket
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

            //lock the ‘free’ send list
            pthread_mutex_lock(&free_list[tag][1][dest].mutex);
            //add the node to the tail of the ‘free’ send list
            if (free_list[tag][1][dest].addTail(node) == -1) {
                printf("writeToSocket: add node to list fail: tag %d, dest %d\n", tag, dest);
                pthread_exit(NULL);
            }
            //unlock the ‘free’ send list
            pthread_mutex_lock(&free_list[tag][1][dest].mutex);
        }
    }
}
