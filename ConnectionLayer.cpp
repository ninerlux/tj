#include <assert.h>
#include <ctype.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <pthread.h>

#include "tcp.h"
#include "ConnectionLayer.h"

// Constructor
ConnectionLayer::ConnectionLayer(const char *conf, const char *domain, int tags) {
    this->tags = tags;

    printf("Setup\n");
    fflush(stdout);

    conn = setupGrid(conf, domain);
    assert(conn != NULL);

    printf("Finished setup\n");
    fflush(stdout);

    int h = 0;
    int t = 0;

    while (conn[h] != NULL) {
        for (t = 0; t < tags && conn[h][t] >= 0; t++);

        if (t == tags) {
            h++;
        } else {
            break;
        }
    }

    local_host = h++;

    // Not sure what the purposes of the next two blocks are
    //     //temporary change-----
    for (t = 0; t < tags; t++) {
        conn[local_host][t] = -2;
    }
    //---------------------

    while (conn[h] != NULL) {
        for (t = 0; t < tags && conn[h][t] >= 0; t++);

        if (t == tags) {
            h++;
        } else {
            break;
        }
    }

    hosts = h;
    printf("hosts = %d\n", hosts);
    server = conn[h + 1][0];
    printf("server = %d\n", server);

    // Print out the connection matrix for debugging
    for (h = 0; h <= hosts + 1 ; h++) {
        for (t = 0; t < tags; t++) {
            printf("%d ", conn[h][t]);
        } 
        printf("\n");
    }

    printf("Init hosts\n");
    fflush(stdout);

    createLists();

    printf("Finished init hosts\n");
    fflush(stdout);

    startThreads();

    printf("Finished connection threads\n");
    fflush(stdout);
}

int ConnectionLayer::get_hosts() {
    return hosts;
}

int ConnectionLayer::get_local_host() {
    return local_host;
}

int **ConnectionLayer::setupGrid(const char *conf, const char *domain) {
    int hosts = 0;
    int ports[MAX_HOSTS];
    char *hostnames[MAX_HOSTS];
    char hostname_and_port[MAX_HOSTS];
    FILE *fp = fopen(conf, "r");

    if (fp == NULL)
        return NULL;

    while (fgets(hostname_and_port, sizeof(hostname_and_port), fp) != NULL) {
        size_t len = strlen(hostname_and_port);

        if (hostname_and_port[len - 1] != '\n')
            return NULL;

        for (len = 0; !isspace(hostname_and_port[len]); len++);

        hostname_and_port[len] = 0;
        hostnames[hosts] = strdup(hostname_and_port);
        ports[hosts] = atoi(&hostname_and_port[len + 1]);
        assert(ports[hosts] > 0);

        if (++hosts == MAX_HOSTS)
            break;
    }

    fclose(fp);

    return tcp_grid_tags(hostnames, ports, hosts, tags, domain);
}

void ConnectionLayer::createLists() {
    int t, p, n, i;

    free_list = new List **[tags];
    busy_list = new HashList **[tags];
    full_list = new List **[tags];

    for (t = 0; t < tags; t++) {
        free_list[t] = new List *[NUM_CONN_TYPES];
        busy_list[t] = new HashList *[NUM_CONN_TYPES];
        full_list[t] = new List *[NUM_CONN_TYPES];

        for (p = 0; p < NUM_CONN_TYPES; p++) {
            free_list[t][p] = new List[hosts];
            busy_list[t][p] = new HashList[hosts];
            full_list[t][p] = new List[hosts];

            for (n = 0; n < hosts; n++) {
                for (i = 0; i < MAX_BLOCKS_PER_LIST; i++) {
                    struct DataBlock db;
                    db.data = malloc(BLOCK_SIZE);
                    memset(db.data, '\0', BLOCK_SIZE);

                    ListNode *node = new ListNode;
                    node->db = db;

                    free_list[t][p][n].addTail(node);
                } 
            }
        }
    }
}

/* spawns (N-1) * T * 2 connection threads and 1 local transfer thread
 * N: node number
 * T: tag number
 * 2: read / write - 0: read connection, 1: write connection
 */
void ConnectionLayer::startThreads() {
    int h, t;

    conn_threads = new pthread_t **[hosts];

    for (h = 0; h < hosts; h++) {
        if (h != local_host) {
            conn_threads[h] = new pthread_t *[tags];

            for (t = 0; t < tags; t++) {
                thr_param *param;
                CLContext *context;
                conn_threads[h][t] = new pthread_t[NUM_CONN_TYPES];

                param = new thr_param();
                param->node = h;
                param->tag = t;
                param->conn = conn[h][t];
                param->conn_type = RECV;

                context = new CLContext();
                context->CL = this;
                context->param = param;
                pthread_create(&conn_threads[h][t][RECV], NULL, &ConnectionLayer::doReadFromSocket, (void *) context);

                param = new thr_param();
                param->node = h;
                param->tag = t;
                param->conn = conn[h][t];
                param->conn_type = SEND;

                context = new CLContext();
                context->CL = this;
                context->param = param;

                pthread_create(&conn_threads[h][t][SEND], NULL, &ConnectionLayer::doWriteToSocket, (void *) context);
            }
        }
    }
}

// Called by a worker thread when it wants to process received blocks
// Returns 0 if no data available
// Returns 1 if data available for tag, and populates db and src to point to DataBlock- and have value of src
int ConnectionLayer::recv_begin(DataBlock *db, int *src, int tag) {
    size_t largest_full_list_size;
    int largest_full_list_index;

    while (true) {
        // Look at all the 'full' receive lists for given tag (N of them)
        // and pick the one with highest 'num' (i.e. longest list).
        // Do this without locking
        largest_full_list_size = 0;
        largest_full_list_index = 0;
        if (full_list[tag] != NULL && full_list[tag][RECV] != NULL) {
            for (int h = 0; h < hosts; h++) {
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
        if (full_list[tag][RECV][largest_full_list_index].getNum() > 0) {
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
void ConnectionLayer::recv_end(DataBlock db, int src, int tag) {
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

    if (src == local_host) {
        // Local transfer: immediately recycle the node to the free send list
        // Lock the 'free' send list for the given src and tag
        pthread_mutex_lock(&free_list[tag][SEND][src].mutex);

		node->db.size = 0;
        // Add that list node to the tail of the 'free' send list
        if (free_list[tag][SEND][src].addTail(node) == -1) {
            printf("recv_end: add node to list fail: tag %d, src %d\n", tag, src);
            exit(-1);
        }

        // Unlock the 'free' send list for the given src and tag
        pthread_mutex_unlock(&free_list[tag][SEND][src].mutex);
    } else {
        // Lock the 'free' receive list for the given src and tag
        pthread_mutex_lock(&free_list[tag][RECV][src].mutex);

		node->db.size = 0;
        // Add that list node to the tail of the 'free' receive list
        if (free_list[tag][RECV][src].addTail(node) == -1) {
            printf("recv_end: add node to list fail: tag %d, src %d\n", tag, src);
            exit(-1);
        }

        // Unlock the 'free' receive list for the given src and tag
        pthread_mutex_unlock(&free_list[tag][RECV][src].mutex);
    }
}


// Called by a worker thread when it is about to start working and needs a place for the output
// Returns 0 if 'free' is empty (checked without locking)
// Returns 1 if data block is available
int ConnectionLayer::send_begin(DataBlock *db, int dest, int tag) {
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
void ConnectionLayer::send_end(DataBlock db, int dest, int tag) {
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

    if (dest == local_host) {
        // Local transfer: immediately move the node to the full receive list
        // Lock the 'full' receive list for the given dest and tag
        pthread_mutex_lock(&full_list[tag][RECV][dest].mutex);

        // Add that list node to the tail of the 'full' receive list
        if (full_list[tag][RECV][dest].addTail(node) == -1) {
            printf("send_end: add node to list fail: tag %d, dest %d\n", tag, dest);
            exit(-1);
        }

        // Unlock the 'full' receive list for the given dest and tag
        pthread_mutex_unlock(&full_list[tag][RECV][dest].mutex);
    } else {
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
}


void *ConnectionLayer::doReadFromSocket(void *context) {
    ConnectionLayer *CL = ((CLContext *) context)->CL;
    return CL->readFromSocket(((CLContext *) context)->param);
}

void *ConnectionLayer::doWriteToSocket(void *context) {
    ConnectionLayer *CL = ((CLContext *) context)->CL;
    return CL->writeToSocket(((CLContext *) context)->param);
}

// Called by the read connection thread
// Can block on read
void *ConnectionLayer::readFromSocket(void *param) {
    thr_param *p = (thr_param *) param;
    ListNode *node = NULL;
    int src = p->node;
    int tag = p->tag;
    int conn_fd = p->conn;

    while (true) {
        // Pull the 1st list node from the 'free' receive list
        pthread_mutex_lock(&free_list[tag][RECV][src].mutex);
        node = free_list[tag][RECV][src].removeHead();
        pthread_mutex_unlock(&free_list[tag][RECV][src].mutex);

        if (node == NULL) {
            continue;
        }

        // Read from socket() in that list node
        int n;
		size_t r = 0;
        while (r < sizeof(node->db.size)) {
            n = read(conn_fd, (char *) &(node->db.size) + r, sizeof(node->db.size) - r);
            if (n < 0) {
                printf("readFromSocket: error on reading from src %d on tag %d\n", src, tag);
                pthread_exit(NULL);
            }

            r += n;
        }

        //node->db.size = size;

        if (node->db.size > BLOCK_SIZE) {
            printf("readFromSocket: error on datablock size %lu from src %d on tag %d\n",
                    node->db.size, src, tag);
            pthread_exit(NULL);
        }

        r = 0;
        while (r < node->db.size) {
            n = read(conn_fd, (char *) node->db.data + r, node->db.size - r);
            if (n < 0) {
                printf("readFromSocket: error on reading from src %d on tag %d\n", src, tag);
                pthread_exit(NULL);
            }
            r += n;
        }

        // Add the list node to the tail of the 'full' receive list
        pthread_mutex_lock(&full_list[tag][RECV][src].mutex);
        if (full_list[tag][RECV][src].addTail(node) == -1) {
            printf("readFromSocket: add node to list fail: tag %d, src %d\n", tag, src);
            pthread_exit(NULL);
        }
        pthread_mutex_unlock(&full_list[tag][RECV][src].mutex);
    }
}

// Called by the write connection thread
void *ConnectionLayer::writeToSocket(void *param) {
    thr_param *p = (thr_param *)param;
    ListNode *node;
    int dest = p->node;
    int tag = p->tag;
    int conn_fd = p->conn;

    while (true) {
        // Pull the 1st node from the 'full' send list
        pthread_mutex_lock(&full_list[tag][SEND][dest].mutex);
        node = full_list[tag][SEND][dest].removeHead();
        pthread_mutex_unlock(&full_list[tag][SEND][dest].mutex);

        if (node == NULL) {
            continue;
        }

		int n;
		size_t r = 0;
		while (r < sizeof(node->db.size)) {
			n = write(conn_fd, (char *) &(node->db.size) + r, sizeof(node->db.size) - r);
			if (n < 0) {
				printf("writeToSocket: error on writing to dest %d on tag %d\n", dest, tag);
				pthread_exit(NULL);
			}
			r += n;
		}

		if (n != sizeof(node->db.size)) {
			printf("writeToSocket: error - partial writing to dest %d on tag %d with size %d\n", dest, tag, n);
			pthread_exit(NULL);
		}

		r = 0;
		while (r < node->db.size) {
			n = write(conn_fd, (char *) node->db.data + r, node->db.size - r);
			if (n < 0) {
				printf("writeToSocket: error on writing to dest %d on tag %d\n", dest, tag);
				pthread_exit(NULL);
			}
			r += n;
		}

		if (r != node->db.size) {
			printf("writeToSocket: error - partial writing to dest %d on tag %d with size %d\n", dest, tag, n);
			pthread_exit(NULL);
		}
        // Add the node to the tail of the 'free' send list
        pthread_mutex_lock(&free_list[tag][SEND][dest].mutex);
		node->db.size = 0;
        if (free_list[tag][SEND][dest].addTail(node) == -1) {
            printf("writeToSocket: add node to list fail: tag %d, dest %d\n", tag, dest);
            pthread_exit(NULL);
        }
        pthread_mutex_unlock(&free_list[tag][SEND][dest].mutex);
    }
}
