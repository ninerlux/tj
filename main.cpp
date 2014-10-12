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

//DN_Queue BLOCKPTRS;
//DN_Queue READ_QUEUE_BY_T[MAX_TAG];
//DN_Queue WRITE_QUEUE_BY_DEST[MAX_NODES];


const char *conf = "/home/xinlu/tj/conf";
const char *domain = ".clic.cs.columbia.edu";
const int tags = 3;
const int conn_type = 2; //two types of connection. 0: read; 1: write.

int **conn;
pthread_t ***conn_threads;
pthread_t *worker_threads;

List ***free_list, ***full_list;
HashList ***busy_list;


void error(const char *msg) {
    perror(msg);
    exit(1);
}



void *readFromSocket(void *param) {
//    thr_param *p = (thr_param *)param;
//    int src = p->host;
//    int tag = p->tag;
//    int conn_fd = p->conn;
//    char *buffer;
//
//    int n;
//
//    while (true) {
//        buffer = new char[BUFFER_SIZE];
//        bzero(buffer, BUFFER_SIZE);
//        n = read(conn_fd, buffer, BUFFER_SIZE);
//        if (n < 0) {
//            error("ERROR reading from socket");
//        }
//
//        recv_enqueue(buffer, tag);
//    }
}


void *writeToSocket(void *param) {
//    thr_param *p = (thr_param *)param;
//    int dest = p->host;
//    int tag = p->tag;
//    int conn_fd = p->conn;
//    char *buffer;
//
//    pthread_mutex_lock(&WRITE_QUEUE_BY_DEST[dest].mutex);
//    //If WRITE_QUEUE_BY_DEST[dest] contains something to write, wake up this thread
//    while (WRITE_QUEUE_BY_DEST[dest].empty) {
//        pthread_cond_wait(&WRITE_QUEUE_BY_DEST[dest].cond, &WRITE_QUEUE_BY_DEST[dest].mutex);
//    }
//    while (WRITE_QUEUE_BY_DEST[dest].head->next != WRITE_QUEUE_BY_DEST[dest].tail) {
//        buffer = (char *)WRITE_QUEUE_BY_DEST[dest].head->next->data;
//        //TODO: ACTUALLY SEND IT -- blocking
//        //TODO: return DataNode to BLOCKPTRS
//    }
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

void printListForward(ListNode *head) {
    while (head != NULL) {
        printf("%lu ", head->db.size);
        head = head->next;
    }
    printf("\n");
}

void printListBackward(ListNode *tail) {
    while (tail != NULL) {
        printf("%lu ", tail->db.size);
        tail = tail->prev;
    }
    printf("\n");
}

void init(int node_nr) {
    int h, t, p;
    free_list = new List **[tags];
    busy_list = new HashList **[tags];
    full_list = new List **[tags];
    for (t = 0; t < tags; t++) {
        free_list[t] = new List *[conn_type];
        busy_list[t] = new HashList *[conn_type];
        full_list[t] = new List *[conn_type];
        for (p = 0; p < conn_type; p++) {
            free_list[t][p] = new List[node_nr];
            busy_list[t][p] = new HashList[node_nr];
            full_list[t][p] = new List[node_nr];
        }
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
    int local_host = h++;
    printf("local host = %d\n", local_host);
    while (conn[h] != NULL) {
        for (t = 0; t < tags && conn[h][t] >= 0; t++);
        if (t == tags) {
	    h++;
	} else {
	    break;
	}
    }
    int hosts = h;
    printf("hosts = %d\n", hosts);
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
