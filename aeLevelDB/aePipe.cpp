//
// Created by dongbo01 on 5/25/17.
//

#include "aePipe.h"
#include <cassert>

int aePipe::sneddata(char *content, int len) {
    int len1 = write(fdIn[1], content, len);
    return len1;
}

int aePipe::setTimeOut(int time) {
    timeOut = time;
    return 0;
}

int aePipe::getdata(char *buffer, int len) {
    return read(fdOut[0], buffer, len);
}


aePipe::aePipe() {
    //pipe(fdIn);

    pthread_t tid;
    pthread_create(&tid, NULL, ListnThread, (void *) this);
    sleep(1);
    Connect((char *) "127.0.0.1", 3000, fdIn[1]);
    std::cout << "pipe:" << fdIn[0] << ":" << fdIn[1] << std::endl;
    pthread_spin_init(&slock, 0);

    rProc = NULL;
    wProc = NULL;
}

aePipe::aePipe(aeFileProc *_rProc, aeFileProc *_wProc) {
    pipe(fdIn);
    pipe(fdOut);

//    pthread_t tid;
//    pthread_create(&tid, NULL, ListnThread, (void *)this);
//    sleep(1);
//    Connect((char *) "127.0.0.1", 3000, fdIn[1]);
//    std::cout <<"pipeIN:"<< fdIn[0] << ":" << fdIn[1] << std::endl;


//    pthread_create(&tid, NULL, ListnThread, (void *)this);
//    sleep(1);
//    Connect((char *) "127.0.0.1", 3000, fdOut[1]);
//    std::cout <<"pipeOut:"<< fdOut[0] << ":" << fdOut[1] << std::endl;

    pthread_spin_init(&slock, 0);

    rProc = _rProc;
    wProc = _wProc;
}

aePipe::~aePipe() {
    close(fdIn[0]);
    close(fdIn[1]);
    close(fdOut[0]);
    close(fdOut[1]);
}


int aePipe::Bind_Listen(const char *bindaddr, int nPort, int &sockfd) {
    int lsockfd = socket(AF_INET, SOCK_STREAM, 0);

    int reuse = 1;
    setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    struct sockaddr_in servaddr;

    servaddr.sin_family = AF_INET;
    servaddr.sin_port = htons(nPort);
    servaddr.sin_addr.s_addr = htonl(INADDR_ANY);

    if (bindaddr && inet_aton(bindaddr, &servaddr.sin_addr) == 0) {
        close(sockfd);
        return -1;
    }

    int nRet = bind(lsockfd, (struct sockaddr *) &servaddr, sizeof(servaddr));
    if (-1 == nRet) {
        close(sockfd);
        return -1;
    }

    nRet = listen(lsockfd, 100);

    struct sockaddr_in cliaddr;
    socklen_t sock_len = sizeof(struct sockaddr_in);
    sockfd = accept(lsockfd, (struct sockaddr *) &cliaddr, &sock_len);

    int rcvbufsize = 1024 * 1024 * 10;
    setsockopt(sockfd, SOL_SOCKET, SO_RCVBUF, &rcvbufsize, sizeof(int));
    int sndbufsize = 1024 * 1024 * 10;
    setsockopt(sockfd, SOL_SOCKET, SO_SNDBUF, &sndbufsize, sizeof(int));
    struct linger ling1 = {1, 0};
    setsockopt(sockfd, SOL_SOCKET, SO_LINGER, &ling1, sizeof(ling1));

    return 0;
}

int aePipe::Connect(char *pRemoteIp, unsigned short nPort, int &sockfd) {
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    int reuse = 1;
    setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    int rcvbufsize = 1024 * 1024 * 10;
    setsockopt(sockfd, SOL_SOCKET, SO_RCVBUF, &rcvbufsize, sizeof(int));

    int sndbufsize = 1024 * 1024 * 10;
    setsockopt(sockfd, SOL_SOCKET, SO_SNDBUF, &sndbufsize, sizeof(int));

    struct sockaddr_in servaddr;
    servaddr.sin_family = AF_INET;
    servaddr.sin_port = htons(nPort);
    servaddr.sin_addr.s_addr = inet_addr(pRemoteIp);

    int nRet = connect(sockfd, (struct sockaddr *) &servaddr, sizeof(servaddr));

    return nRet;
}


void *aePipe::ListnThread(void *arg) {
    aePipe *me = (aePipe *) arg;
    me->Bind_Listen("127.0.0.1", 3000, me->fdIn[0]);

    return NULL;
}





