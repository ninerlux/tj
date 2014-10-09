#include <iostream>
#include <assert.h>
#include <string.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "tcp.h"
#include "usertype.h"

using namespace std;

DN_Queue BLOCKPTRS;
DN_Queue READ_QUEUE_BY_T[MAX_TAG];
DN_Queue WRITE_QUEUE_BY_DEST[MAX_NODES];


const char *conf = "/home/xinlu/tj/conf";
const char *domain = ".clic.cs.columbia.edu";
const size_t tags = 3;

int **conn;
pthread_t ***conn_threads;
pthread_t *worker_threads;

void error(const char *msg) {
    perror(msg);
    exit(1);
}


void recv_enqueue(char *buffer, int tag) {
    //Grab an empty DataNode from BLOCKPTRS
    pthread_mutex_lock(&BLOCKPTRS.mutex);
    while (BLOCKPTRS.processing) {
        pthread_cond_wait(&BLOCKPTRS.cond, &BLOCKPTRS.mutex);
    }
    BLOCKPTRS.processing = true;
    DataNode *data_ptr = BLOCKPTRS.tail;
    BLOCKPTRS.tail = BLOCKPTRS.tail->prev;
    data_ptr->prev = NULL;
    data_ptr->next = NULL;
    pthread_mutex_unlock(&BLOCKPTRS.mutex);

    //Acquire READ_QUEUE_BY_T[tag]'s mutex and enqueue the new empty DataNode
    pthread_mutex_lock(&READ_QUEUE_BY_T[tag].mutex);
    READ_QUEUE_BY_T[tag].tail->prev->next = data_ptr;
    data_ptr->prev = READ_QUEUE_BY_T[tag].tail->prev;
    READ_QUEUE_BY_T[tag].tail->prev = data_ptr;
    data_ptr->next = READ_QUEUE_BY_T[tag].tail;
    //fill in data
    data_ptr->data = (void *)buffer;
    pthread_mutex_unlock(&READ_QUEUE_BY_T[tag].mutex);
}

void *readFromSocket(void *param) {
    thr_param *p = (thr_param *)param;
    int src = p->host;
    int tag = p->tag;
    int conn_fd = p->conn;
    char *buffer;

    int n;

    while (true) {
        buffer = new char[BUFFER_SIZE];
        bzero(buffer, BUFFER_SIZE);
        n = read(conn_fd, buffer, BUFFER_SIZE);
        if (n < 0) {
            error("ERROR reading from socket");
        }

        recv_enqueue(buffer, tag);
    }
}


void *writeToSocket(void *param) {
    thr_param *p = (thr_param *)param;
    int dest = p->host;
    int tag = p->tag;
    int conn_fd = p->conn;
    char *buffer;

    pthread_mutex_lock(&WRITE_QUEUE_BY_DEST[dest].mutex);
    //If WRITE_QUEUE_BY_DEST[dest] contains something to write, wake up this thread
    while (WRITE_QUEUE_BY_DEST[dest].empty) {
        pthread_cond_wait(&WRITE_QUEUE_BY_DEST[dest].cond, &WRITE_QUEUE_BY_DEST[dest].mutex);
    }
    while (WRITE_QUEUE_BY_DEST[dest].head->next != WRITE_QUEUE_BY_DEST[dest].tail) {
        buffer = (char *)WRITE_QUEUE_BY_DEST[dest].head->next->data;
        //TODO: ACTUALLY SEND IT -- blocking
        //TODO: return DataNode to BLOCKPTRS
    }
}

//void *worker() {
//    void *input_block;
//    void *output_block;
//
//    while (true) {
//        if (recv_dequeue(&input_block, &src, tag)) {
//
//        } else {
//
//        }
//    }
//}

int **setup(const char *conf_filename, const char *domain, size_t tags, size_t max_hosts) {
    size_t hosts = 0;
    int ports[max_hosts];
    char *hostnames[max_hosts];
    char hostname_and_port[max_hosts];
    FILE *fp = fopen(conf_filename, "r");
    if (fp == NULL) return NULL;
    while (fgets(hostname_and_port, sizeof(hostname_and_port), fp) != NULL) {
        size_t len = strlen(hostname_and_port);
        if (hostname_and_port[len - 1] != '\n') return NULL;
        for (len = 0; !isspace(hostname_and_port[len]); len++);
        hostname_and_port[len] = 0;
        hostnames[hosts] = strdup(hostname_and_port);
        ports[hosts] = atoi(&hostname_and_port[len + 1]);
        assert(ports[hosts] > 0);
        if (++hosts == max_hosts) break;
    }
    fclose(fp);
    return tcp_grid_tags(hostnames, ports, hosts, tags, domain);
}

void printListForward(DataNode *head) {
    while (head != NULL) {
        printf("%d ", head->tag);
        head = head->next;
    }
    printf("\n");
}

void printListBackward(DataNode *tail) {
    while (tail != NULL) {
        printf("%d ", tail->tag);
        tail = tail->prev;
    }
    printf("\n");
}

void init(int node_nr) {
    /* init BLOCKPTRS */
    BLOCKPTRS.head = new DataNode();
    BLOCKPTRS.tail = new DataNode();
    BLOCKPTRS.head->next = BLOCKPTRS.tail;
    BLOCKPTRS.tail->prev = BLOCKPTRS.head;
    DataNode *block;
    for (int i = 0; i < AVAL_BLOCKS; i++) {
        block = new DataNode(BLOCK_SIZE);
        block->tag = i;
        block->next = BLOCKPTRS.head->next;
        BLOCKPTRS.head->next->prev = block;
        BLOCKPTRS.head->next = block;
        block->prev = BLOCKPTRS.head;
    }
    //printListForward(BLOCKPTRS.head);
    //printListBackward(BLOCKPTRS.tail);
    //printf("head = %d ", (BLOCKPTRS.head)->tag);
    //printf("tail = %d ", BLOCKPTRS.tail->tag);

    /* init READ_QUEUE_BY_T */
    for (int t = 0; t < tags; t++) {
        READ_QUEUE_BY_T[t].head = new DataNode();
        READ_QUEUE_BY_T[t].tail = new DataNode();
        READ_QUEUE_BY_T[t].head->next = READ_QUEUE_BY_T[t].tail;
        READ_QUEUE_BY_T[t].tail->prev = READ_QUEUE_BY_T[t].head;
    }
    /* init WRITE_QUEUE_BY_DEST */
    for (int h = 0; h < node_nr; h++) {
        WRITE_QUEUE_BY_DEST[h].head = new DataNode();
        WRITE_QUEUE_BY_DEST[h].tail = new DataNode();
        WRITE_QUEUE_BY_DEST[h].head->next = WRITE_QUEUE_BY_DEST[h].tail;
        WRITE_QUEUE_BY_DEST[h].tail->prev = WRITE_QUEUE_BY_DEST[h].head;
    }

}

int main(int argc, char** argv) {
    //set up connections
    printf("Setup\n");
    fflush(stdout);
    conn = setup(conf, domain, tags, 255);
    printf("Finished setup\n");
    fflush(stdout);
    assert(conn != NULL);
    size_t h = 0;
    size_t t = 0;
    while (conn[h] != NULL) {
        for (t = 0; t < tags && conn[h][t] >= 0; t++);
        if (t == tags) {
	    h++;
	} else {
	    break;
	}
    }
    size_t local_host = h++;
    printf("local host = %lu\n", local_host);
    while (conn[h] != NULL) {
        for (t = 0; t < tags && conn[h][t] >= 0; t++);
        if (t == tags) {
	    h++;
	} else {
	    break;
	}
    }
    size_t hosts = h;
    printf("hosts = %lu\n", hosts);
    int server = conn[h + 1][0];
    printf("server = %d\n", server);
    //print out the connection matrix
    for (h = 0; h <= hosts + 1 ; h++) {
	for (t = 0; t < tags; t++) {
	    printf("%d ", conn[h][t]);
	} 
	printf("\n");
    }

    printf("Init hosts\n");
    fflush(stdout);
    init(hosts);
    printf("Finished init hosts\n");
    fflush(stdout);

    /* spawn N * T * 2 connection threads
     * N: node number
     * T: tag number
     * 2: read / write - 0: read connection, 1: write connection
     */
//    int k = 0;
//    conn_threads = new pthread_t **[hosts];
//    for (h = 0; h < hosts; h++) {
//        conn_threads[h] = new pthread_t *[tags];
//        for (t = 0; t < tags; t++) {
//            conn_threads[h][t] = new pthread_t[2];
//            thr_param *param = new thr_param();
//            param->host = h;
//            param->tag = t;
//            param->conn = conn[h][t];
//            pthread_create(&conn_threads[h][t][0], NULL, readFromSocket, (void *)param);
//            pthread_create(&conn_threads[h][t][1], NULL, writeToSocket, (void *)param);
//
//        }
//    }
//    /* spawn T worker threads
//     * T: tag number
//     */
//    worker_threads = new pthread_t[tags];
//    for (t = 0; t < tags; t++) {
//        pthread_create(&worker_threads[t], NULL, worker, NULL);
//    }



    return 0;
}
