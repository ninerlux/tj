#include <iostream>
#include <assert.h>
#include <string.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "tcp.h"
#include "usertype.h"
#include "ConnThread.h"

using namespace std;

const char *conf = "/home/ajk2214/cs6901/tj/conf";
const char *domain = ".clic.cs.columbia.edu";
const int tags = 2;
const int conn_type = 2; // Two types of connection. 0: read; 1: write.

int local_host;
int hosts;
int server;

int **conn;
pthread_t ***conn_threads;
pthread_t *worker_threads;

List ***free_list, ***full_list;
HashList ***busy_list;

void error(const char *info) {
    perror(info);
    exit(1);
}


void *worker(void *param) {
    thr_param *p = (thr_param *) param;
    int tag = p->tag;
    int m;
    int n;
    DataBlock db;

    if (tag == 0) {
        // Send something to each node, followed by a termination message
        for (n = 0; n < hosts; n++) {
            for (m = 0; m < 100; m++) {
                while(!send_begin(&db, n, tag+1));
                sprintf((char *) db.data, "Test message %d from %d", m, local_host);
                db.size = strlen((const char *) db.data) + 1;
                send_end(db, n, tag+1);
            }

            while(!send_begin(&db, n, tag+1));
            db.size = 0;
            send_end(db, n, tag+1);
        }
    } else if (tag == 1) {
        // Receive until termination received from all nodes
        DataBlock db;
        int src;
        int t = 0;

        while (t != hosts) {
            while (!recv_begin(&db, &src, tag));

            if (db.size > 0) {
                printf("Node %d received \"%s\" %p from node %d\n", local_host, (char *) db.data, db.data, src);
                fflush(stdout);
            } else {
                t++;
            }

            recv_end(db, src, tag);
        }
    }

    return NULL;
}

int **setup(const char *conf_filename, const char *domain, size_t tags, size_t max_hosts) {
    size_t hosts = 0;
    int ports[max_hosts];
    char *hostnames[max_hosts];
    char hostname_and_port[max_hosts];
    FILE *fp = fopen(conf_filename, "r");

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

        if (++hosts == max_hosts)
            break;
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
    int t, p, n, i;

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

            for (n = 0; n < node_nr; n++) {
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

int main(int argc, char** argv) {
    // Set up connections
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

    local_host = h++;
    printf("local host = %d\n", local_host);
    
    // Not sure what the purposes of the next two blocks are
    //temporary change-----
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

    init(hosts);

    printf("Finished init hosts\n");
    fflush(stdout);

    /* spawn (N-1) * T * 2 connection threads and 1 local transfer thread
     * N: node number
     * T: tag number
     * 2: read / write - 0: read connection, 1: write connection
     * The ConnThread methods differentiate data transfer between a node to
     * itself and a node to other nodes
     */
    conn_threads = new pthread_t **[hosts];

    for (h = 0; h < hosts; h++) {
        if (h != local_host) {
            conn_threads[h] = new pthread_t *[tags];

            for (t = 0; t < tags; t++) {
                printf("Creating read/write threads %d %d\n", h, t);
                fflush(stdout);

                thr_param *param;
                conn_threads[h][t] = new pthread_t[conn_type];

                param = new thr_param();
                param->node = h;
                param->tag = t;
                param->conn = conn[h][t];
                param->conn_type = RECV;
                pthread_create(&conn_threads[h][t][RECV], NULL, &readFromSocket, (void *) param);

                param = new thr_param();
                param->node = h;
                param->tag = t;
                param->conn = conn[h][t];
                param->conn_type = SEND;
                pthread_create(&conn_threads[h][t][SEND], NULL, &writeToSocket, (void *) param);
            }
        }
    }

    /* spawn T worker threads
     * T: tag number
     */
    worker_threads = new pthread_t[tags];
    for (t = 0; t < tags; t++) {
        thr_param *param;
        param = new thr_param();
        param->tag = t;
        pthread_create(&worker_threads[t], NULL, &worker, (void *) param);
    }

    for (t = 0; t < tags; t++) {
        void *retval;
        pthread_join(worker_threads[t], &retval);
    }

    return 0;
}
