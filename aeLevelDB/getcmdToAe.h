//
// Created by dongbo01 on 5/25/17.
//

#ifndef AEPOLL_GETCMDTOAE_H
#define AEPOLL_GETCMDTOAE_H

#include <list>
#include <map>
#include <vector>
#include "opleveldb.h"
#include "aEpoll/ae.h"
#include "aePipe.h"
#include  "job.h"

class getcmdToAe {
public:
    aePipe *pipeLine;
    static void getcmdToAeInit(int num);
    ~getcmdToAe();

public:
    static std::vector<getcmdToAe *> SvrList;


private:
    static void *Run(void *arg);

    static std::vector<getcmdToAe *> createlist() {
        std::vector<getcmdToAe *> _SvrList;
        return _SvrList;
    }

    getcmdToAe(int id);

    int GetcmdtimeNOut(JOB *job);

    int GetcmdtimeOut();

    static void recvn(aeEventLoop *el, int fd, void *privdata, int mask);
    static void sendn(aeEventLoop *el, int fd, void *privdata, int mask);
    static void timeOutProc(void *arg);

    int getdata(int fd, char *buffer, int len) ;
    int sneddata(int fd,char *content, int len) ;
    int setTimeOut(int time) ;

private:

    aeEventLoop *el;
    std::map<std::string, std::map<int, opleveldb *> > doCmdFindDB;
    int id;
    static int numSvr;

    std::map<std::string, std::map<int, opleveldb *> > opPushlist1;
    std::map<std::string, std::map<int, opleveldb *> > opPushlist2;

    std::list<JOB*> pushlist;
};


#endif //AEPOLL_GETCMDTOAE_H
