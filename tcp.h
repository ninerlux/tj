#ifndef _TCP_H_
#define _TCP_H_


/* -1 on failure */
int tcp_server(int port);

/* -1 on failure, -2 on timeout */
int tcp_accept(int server, char *host,
               int *port, int timeout_us);

/* -1 on failure, -2 on timeout */
int tcp_connect(const char *host, int port,
                int timeout_us);

/* -1 on self, -1 on after end */
int *tcp_grid(char **hostnames, int *ports,
              int hosts, const char *domain);

/* dedicated connection for each tag for each pair of nodes
 * -1 on self, -1 on after end
 */
int **tcp_grid_tags(char **hostnames, int *ports,
        int hosts, int tags, const char *domain);

#endif
