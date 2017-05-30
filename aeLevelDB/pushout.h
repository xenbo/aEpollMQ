//
// Created by dongbo01 on 5/23/17.
//

#ifndef MQSVR2_PUSHOUT_H
#define MQSVR2_PUSHOUT_H

#include <vector>
#include "aePipe.h"
#include "job.h"
#include "aEpoll/ae.h"

#include "protocol/server/MQLBSvrXY.h"
#include "protocol/server/MQSvrXY.h"
#include "protocol/30/ManageXY.h"
#include "common/ProtocolHead.h"

class pushout {

public:
    int setzpipe(aePipe *pipe);

    int Start();

    aeEventLoop *el;

private:
    static void *Run(void *arg);

    static void push(aeEventLoop *el, int fd, void *privdata, int mask);

private:
    std::vector<aePipe *> zpipelist;

};


#endif //MQSVR2_PUSHOUT_H
