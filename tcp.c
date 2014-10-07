#include <sys/types.h>
#include <sys/socket.h>
#include <sys/select.h>
#include <sys/time.h>
#include <unistd.h>
#include <fcntl.h>
#include <netdb.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int tcp_server(int port)
{
	char service[12];
	int cls, server = -1;
	struct addrinfo hints, *addr_list, *p;
	/* set specifications */
	sprintf(service, "%d", port);
	memset(&hints, 0, sizeof(struct addrinfo));
	hints.ai_flags = AI_PASSIVE;
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	/* get a list of matching configurations */
	if (getaddrinfo(NULL, service, &hints, &addr_list))
		return -1;
	/* loop over possible configurations */
	for (p = addr_list ; p != NULL ; p = p->ai_next) {
		/* setup socket using configuration */
		if ((server = socket(p->ai_family, p->ai_socktype,
		                     p->ai_protocol)) < 0) continue;
		/* set socket options, bind and set listen queue */
		if (!bind(server, p->ai_addr, p->ai_addrlen) &&
		    !listen(server, 128)) break;
		/* on failure close socket and try next case */
		cls = close(server);
		server = -1;
		if (cls) break;
	}
	freeaddrinfo(addr_list);
	return server;
}

int tcp_accept(int server, char *host, int *port, int timeout_us)
{
	fd_set select_set;
	struct sockaddr addr;
	int sel, ver, flags, client = -1;
	socklen_t addr_len = sizeof(struct sockaddr);
	/* space for host and port */
	char host_s[NI_MAXHOST];
	char port_s[NI_MAXSERV];
	/* set timeout */
	struct timeval tv, *tv_p = NULL;
	if (timeout_us >= 0) {
		tv_p = &tv;
		tv.tv_sec  = timeout_us / 1000000;
		tv.tv_usec = timeout_us % 1000000;
	}
	/* set server as non-blocking and get protocol */
	if ((flags = fcntl(server, F_GETFL, 0)) < 0 ||
	    (flags & O_NONBLOCK) ||
	    fcntl(server, F_SETFL, flags | O_NONBLOCK) ||
	    getsockname(server, &addr, &addr_len))
		return -1;
	ver = addr.sa_family;
	/* wait until ready or timeout */
	FD_ZERO(&select_set);
	FD_SET(server, &select_set);
	sel = select(server + 1, &select_set, NULL, NULL, tv_p);
	/* differentiate error and timeout */
	if (sel != 1) return !sel ? -2 : -1;
	/* accept pending connection */
	addr_len = sizeof(struct sockaddr);
	client = accept(server, &addr, &addr_len);
	if (client < 0) return -1;
	/* get info on connection and use numeric values */
	addr.sa_family = ver;
	if (fcntl(server, F_SETFL, flags) ||
	    getnameinfo(&addr, sizeof(struct sockaddr),
	                host_s, NI_MAXHOST, port_s, NI_MAXSERV,
	                /* NI_NUMERICHOST | */ NI_NUMERICSERV)) {
		close(client);
		return -1;
	}
	/* return host and port if required */
	if (host != NULL) strcpy(host, host_s);
	if (port != NULL) *port = atoi(port_s);
	return client;
}

int tcp_connect(const char *host, int port, int timeout_us)
{
	char service[12];
	struct addrinfo hints, *addr_list, *p;
	int flags, ntime, ptime = 0, cls = 0, sel = -1, conn = -1;
	struct timeval tv, *tv_p = timeout_us < 0 ? NULL : &tv;
	/* set specifications */
	sprintf(service, "%d", port);
	memset(&hints, 0, sizeof(struct addrinfo));
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	/* keep track of time */
	if (timeout_us > 0) {
		gettimeofday(&tv, NULL);
		ptime = tv.tv_sec * 1000000 + tv.tv_usec;
	}
	/* get a list of matching configurations */
	if (getaddrinfo(host, service, &hints, &addr_list))
		return -1;
	/* loop over possible configurations */
	for (p = addr_list ; p != NULL ; p = p->ai_next) {
		/* create socket using configuration */
		if ((conn = socket(p->ai_family, p->ai_socktype,
		                   p->ai_protocol)) < 0) continue;
		/* set socket as non-blocking and try to connect */
		if ((flags = fcntl(conn, F_GETFL, 0)) >= 0 &&
		    !(flags & O_NONBLOCK) &&
		    !fcntl(conn, F_SETFL, flags | O_NONBLOCK)) {
			int status = connect(conn, p->ai_addr, p->ai_addrlen);
			/* if already connected stop tries */
			if (!status && !fcntl(conn, F_SETFL, flags)) break;
			/* if still in progress */
			if (status < 0 && errno == EINPROGRESS) {
				socklen_t addr_len = sizeof(int);
				fd_set select_set;
				/* set timeout */
				if (timeout_us >= 0) {
					tv.tv_sec  = timeout_us / 1000000;
					tv.tv_usec = timeout_us % 1000000;
				}
				/* wait until ready or timeout */
				FD_ZERO(&select_set);
				FD_SET(conn, &select_set);
				if (!(sel = select(conn + 1, NULL, &select_set,
				                   NULL, tv_p))) timeout_us = 0;
				/* update remaining time */
				if (timeout_us > 0) {
					gettimeofday(&tv, NULL);
					ntime = tv.tv_sec * 1000000 + tv.tv_usec;
					timeout_us -= ntime - ptime;
					if (timeout_us < 0) timeout_us = 0;
					ptime = ntime;
				}
				/* check connection result */
				if (sel == 1 &&
				    !getsockopt(conn, SOL_SOCKET, SO_ERROR,
				                (void*) &status, &addr_len) &&
				    !status &&
				    !fcntl(conn, F_SETFL, flags)) break;
			}
		}
		/* on failure close socket and try next case */
		cls = close(conn);
		conn = -1;
		if (cls) break;
	}
	freeaddrinfo(addr_list);
	return !sel && !cls ? -2 : conn;
}

int *tcp_grid(char **hostnames, int *ports,
              int hosts, const char *domain)
{
	int h, server = -1, *conn = NULL;
	if (conn != NULL) {
error:
		if (server >= 0)
			close(server);
		for (h = 0 ; h != hosts ; ++h)
			if (conn != NULL && conn[h] >= 0)
				close(conn[h]);
		free(conn);
		return NULL;
	}
	// get local hostname
	char hostname[NI_MAXHOST];
	char hostname_domain[NI_MAXHOST];
	if (gethostname(hostname, NI_MAXHOST)) {
		fprintf(stderr, "???: Cannot get hostname from OS\n");
		goto error;
	}
	// check that domain fits
	for (h = 0 ; h != hosts ; ++h)
		if (strlen(hostnames[h]) + strlen(domain) >= NI_MAXHOST) {
			fprintf(stderr, "%s: Possibly invalid domain\n", hostname);
			goto error;
		}
	// find host in file
	for (h = 0 ; h != hosts ; ++h)
		if (!strcmp(hostnames[h], hostname)) break;
	int local_host = h;
	if (h == hosts) {
		fprintf(stderr, "%s: Cannot find hostname in hosts\n", hostname);
		goto error;
	}
	// open server if not last
	server = tcp_server(ports[local_host]);
	if (server < 0) {
		fprintf(stderr, "%s: Node %d cannot open server (errno: %d)\n",
		        hostnames[local_host], local_host, errno);
		goto error;
	}
	// allocate connection array
	conn = (int*) malloc((hosts + 2) * sizeof(int));
	for (h = 0 ; h <= hosts ; ++h)
		conn[h] = -1;
	// connect to all previous nodes
	int timeout = -1;
	for (h = 0 ; h != local_host ; ++h) {
		conn[h] = tcp_connect(hostnames[h], ports[h], timeout);
		if (conn[h] < 0) {
			fprintf(stderr, "%s: Node %d cannot connect to node %d (errno: %d)\n",
			        hostnames[local_host], local_host, h, errno);
			goto error;
		}
	}
	// wait for all next nodes to connect to you
	int hosts_after = hosts - local_host - 1;
	while (hosts_after--) {
		int new_conn = tcp_accept(server, hostname, NULL, timeout);
		if (new_conn < 0) {
			fprintf(stderr, "%s: Node %d received invalid connection (errno: %d)\n",
			        hostnames[local_host], local_host, errno);
			goto error;
		}
		// search which host connected and verify
		for (h = local_host + 1 ; h != hosts ; ++h) {
			strcpy(hostname_domain, hostnames[h]);
			strcat(hostname_domain, domain);
			if (!strcmp(hostname_domain, hostname)) break;
		}
		if (h == hosts) {
			fprintf(stderr, "%s: Node %d cannot accept connection from unknown node \"%s\" (errno: %d)\n",
			        hostnames[local_host], local_host, hostname, errno);
			goto error;
		}
		if (conn[h] >= 0) {
			fprintf(stderr, "%s: Node %d already accepted connection from node %d (errno: %d)\n",
			        hostnames[local_host], local_host, h, errno);
			goto error;
		}
		conn[h] = new_conn;
	}
	conn[hosts + 1] = server;
//	if (close(server)) {
//		fprintf(stderr, "%s: Node %d server socket not closed correctly (errno: %d)\n",
//		        hostnames[local_host], local_host, errno);
//		goto error;
//	}
	return conn;
}

int **tcp_grid_tags(char **hostnames, int *ports,
              int hosts, int tags, const char *domain)
{
    int h, t, server = -1, **conn = NULL;
    if (conn != NULL) {
        error:
        if (server >= 0)
            close(server);
        for (h = 0 ; h != hosts ; ++h)
            for (t = 0; t != tags; ++t)
                if (conn != NULL && conn[h] != NULL && conn[h][t] >= 0)
                    close(conn[h][t]);
        free(conn);
        return NULL;
    }
    // get local hostname
    char hostname[NI_MAXHOST];
    char hostname_domain[NI_MAXHOST];
    if (gethostname(hostname, NI_MAXHOST)) {
        fprintf(stderr, "???: Cannot get hostname from OS\n");
        goto error;
    }
    // check that domain fits
    for (h = 0 ; h != hosts ; ++h)
        if (strlen(hostnames[h]) + strlen(domain) >= NI_MAXHOST) {
            fprintf(stderr, "%s: Possibly invalid domain\n", hostname);
            goto error;
        }
    // find host in file
    for (h = 0 ; h != hosts ; ++h)
        if (!strcmp(hostnames[h], hostname)) break;
    int local_host = h;
    if (h == hosts) {
        fprintf(stderr, "%s: Cannot find hostname in hosts\n", hostname);
        goto error;
    }
    // open server if not last
    server = tcp_server(ports[local_host]);
    if (server < 0) {
        fprintf(stderr, "%s: Node %d cannot open server (errno: %d)\n",
                hostnames[local_host], local_host, errno);
        goto error;
    }
    // allocate connection array

    conn = new int *[hosts + 2];
    for (h = 0; h != hosts ; ++h) {
        conn[h] = new int[tags + 1];
    }

    for (h = 0; h <= hosts + 1; ++h) {
        for (t = 0; t <= tags; ++t) {
            conn[h][t] = -1;
        }
    }
    // connect to all previous nodes
    int timeout = -1;
    for (h = 0 ; h != local_host ; ++h) {
        for (t = 0; t != tags; ++t) {
            conn[h][t] = tcp_connect(hostnames[h], ports[h], timeout);
            if (conn[h] < 0) {
                fprintf(stderr, "%s: Node %d cannot connect to node %d (errno: %d)\n",
                        hostnames[local_host], local_host, h, errno);
                goto error;
            }
        }
    }
    // wait for all next nodes to connect to you
    int hosts_after = hosts - local_host - 1;
    while (hosts_after--) {
        for (t = 0; t < tags; t++) {
            int new_conn = tcp_accept(server, hostname, NULL, timeout);
            if (new_conn < 0) {
                fprintf(stderr, "%s: Node %d received invalid connection (errno: %d)\n",
                        hostnames[local_host], local_host, errno);
                goto error;
            }
            // search which host connected and verify
            for (h = local_host + 1; h != hosts; ++h) {
                strcpy(hostname_domain, hostnames[h]);
                strcat(hostname_domain, domain);
                if (!strcmp(hostname_domain, hostname)) break;
            }
            if (h == hosts) {
                fprintf(stderr, "%s: Node %d cannot accept connection from unknown node \"%s\" (errno: %d)\n",
                        hostnames[local_host], local_host, hostname, errno);
                goto error;
            }
            if (conn[h][t] >= 0) {
                fprintf(stderr, "%s: Node %d already accepted connection from node %d (errno: %d)\n",
                        hostnames[local_host], local_host, h, errno);
                goto error;
            }
            conn[h][t] = new_conn;
        }
    }

    conn[hosts + 1][0] = server;

    return conn;
}