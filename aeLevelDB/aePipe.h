//
// Created by dongbo01 on 5/25/17.
//

#ifndef MQSVR2_AEPIPE_H
#define MQSVR2_AEPIPE_H

#include <iostream>
#include <iostream>
#include <cstdio>
#include<unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <errno.h>
#include <sys/syscall.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <pthread.h>
#include "aEpoll/ae.h"


class aePipe {

public:
    int fdIn[2];
    int fdOut[2];
    int timeOut;

    pthread_spinlock_t slock;
public:
    int sneddata(char *content, int len);
    int getdata(char *buffer, int len);
    int setTimeOut(int time);
    aePipe();
    aePipe(aeFileProc *_rProc, aeFileProc *_wProc);
    ~aePipe();

    int getfdIn(){ return  fdIn[0];}
    int getfdOut() { return  fdOut[0];}

    aeFileProc *rProc;
    aeFileProc *wProc;


private:
    static  void *ListnThread(void *arg);
    int Bind_Listen(const char *bindaddr, int nPort, int &sockfd);
    int Connect(char *pRemoteIp, unsigned short nPort, int &sockfd);
};







#endif //MQSVR2_AEPIPE_H
