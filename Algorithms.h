#ifndef _ALGORITHMS_H
#define _ALGORITHMS_H

#include "ConnectionLayer.h"
#include "usertype.h"

using namespace std;

class AbstractAlgo {
public:
    AbstractAlgo() {};

    virtual int get_tags() = 0;
    virtual int run(ConnectionLayer *CL, struct table_r R, struct table_s S) = 0;

protected:
    pthread_t *worker_threads;
};

class ProducerConsumer : public AbstractAlgo {
public:
    int get_tags();
    int run(ConnectionLayer *CL, struct table_r R, struct table_s S);
};

#endif
