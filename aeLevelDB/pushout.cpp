//
// Created by dongbo01 on 5/23/17.
//
#include <cstring>
#include <cstdlib>
#include "pushout.h"


#include <assert.h>

int pushout::setzpipe(aePipe *pipe) {
    zpipelist.push_back(pipe);
    return zpipelist.size();
}

void pushout::push(aeEventLoop *el, int fd, void *privdata, int mask) {

    unsigned long buf[4096];
    int len = read(fd, (char *) buf, 4096 *8);
    for (unsigned int j = 0; j < len / sizeof(unsigned long); j++) {
        JOB *eTask = (JOB *) buf[j];

        bistream bis;
        bis.attach(eTask->p, eTask->slen);
        Protocol::V32::MQSvr::SendMsg msg;
        bis >> msg;

        std::cout << "xyid:" << eTask->xyid << ",msg=" << msg.szContent << std::endl;

        char mess[4096];
        OldClientHead *head = (OldClientHead *) mess;
        head->xyid = Protocol::V32::MQSvr::SendMsg::XY_ID;
        head->len = eTask->slen;
        memcpy(mess + 4, eTask->p, eTask->slen);

        CIOSocket *Socket = static_cast<CIOSocket *>(eTask->t);
        assert(Socket != NULL);
        Socket->AsynSend(mess, head->len + 4);

        delete eTask;
    }
}

void *pushout::Run(void *arg) {
    pushout *me = static_cast<pushout *>(arg);

    me->el = aeCreateEventLoop(65535);
    me->el->timeout = -1;
    me->el->timeOutdo = NULL;
    me->el->t = (void *) me;
    for (unsigned int i = 0; i < me->zpipelist.size(); i++) {
        aeCreateFileEvent(me->el, me->zpipelist[i]->fdOut[0], AE_READABLE, push, NULL);
    }

    std::cout << "push start............." << std::endl;
    aeMain(me->el);
    aeDeleteEventLoop(me->el);

    return NULL;
}

int pushout::Start() {
    pthread_t tid;
    pthread_create(&tid, NULL, Run, (void *) this);
    return 0;
}
