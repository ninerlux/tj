#ifndef _ALGORITHMS_H
#define _ALGORITHMS_H

#include "ConnectionLayer.h"
#include "usertype.h"

using namespace std;

class AbstractAlgo {
public:
    AbstractAlgo() {};

    virtual int get_tags() = 0;
    virtual int run(ConnectionLayer *, struct table_r *, struct table_s *) = 0;

protected:
    pthread_t *worker_threads;
};

class ProducerConsumer : public AbstractAlgo {
public:
    int get_tags();
    int run(ConnectionLayer *, struct table_r *, struct table_s *);
};

class HashJoin : public AbstractAlgo {
public:
    int get_tags();
    int run(ConnectionLayer *, struct table_r *, struct table_s *);
};

class TrackJoin2 : public AbstractAlgo {
public:
	int get_tags();
	int run(ConnectionLayer *, struct table_r *, struct table_s *);
};

class TrackJoin4 : public AbstractAlgo {
public:
    int get_tags();
    int run(ConnectionLayer *, struct table_r *, struct table_s *);
};

#endif
