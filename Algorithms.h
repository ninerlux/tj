#ifndef _ALGORITHMS_H
#define _ALGORITHMS_H

#include "ConnectionLayer.h"

using namespace std;

class AbstractAlgo {
public:
    AbstractAlgo() {};

    virtual int get_tags() = 0;
    virtual int run(ConnectionLayer *CL) = 0;

protected:
    pthread_t *worker_threads;
};

class ProducerConsumer : public AbstractAlgo {
public:
    int get_tags();
    int run(ConnectionLayer *CL);
};

#endif
