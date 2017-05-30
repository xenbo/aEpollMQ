//
// Created by dongbo01 on 5/25/17.
//

#include <cstdio>
#include <cassert>
#include "getcmdToAe.h"
//#include "protocol/server/MQLBSvrXY.h"
//#include "protocol/server/MQSvrXY.h"
//#include "protocol/30/ManageXY.h"
//#include "common/ProtocolHead.h"


int  getcmdToAe::numSvr = 0;
std::vector<getcmdToAe *> getcmdToAe::SvrList = getcmdToAe::createlist();

int getcmdToAe::getdata(int fd, char *buffer, int len) {
    return read(fd, buffer, len);
}

int getcmdToAe::sneddata(int fd, char *content, int len) {
    return write(fd, content, len);
}

int getcmdToAe::setTimeOut(int time) {
    el->timeout = time;
    return 0;
}

void getcmdToAe::recvn(aeEventLoop *el, int fd, void *privdata, int mask) {
    getcmdToAe *me = (getcmdToAe *) el->t;

    unsigned long buf[3000];
    int len = me->getdata(fd, (char *) buf, 3000 * 8);
    for (unsigned int i = 0; i < len / sizeof(unsigned long); i++) {

        JOB *eTask = (JOB *) buf[i];
        me->GetcmdtimeNOut(eTask);
    }//for
}

void getcmdToAe::sendn(aeEventLoop *el, int fd, void *privdata, int mask) {
    getcmdToAe *me = (getcmdToAe *) el->t;
    while (!me->pushlist.empty()) {
        JOB *job = me->pushlist.front();
        unsigned long addr = (unsigned long) job;
        // write(fd, (char *) &addr, sizeof(unsigned long));
        me->pushlist.pop_front();
        std::cout << time(0) << " topic" << job->topic << ",len:" << job->slen << std::endl;
    }

    aeCreateFileEvent(me->el, me->pipeLine->fdIn[0], AE_READABLE, me->recvn, NULL);
    aeDeleteFileEvent(el, fd, AE_WRITABLE);
}

void getcmdToAe::timeOutProc(void *arg) {
    getcmdToAe *me = (getcmdToAe *) arg;
    me->GetcmdtimeOut();
}


void getcmdToAe::getcmdToAeInit(int num) {
    numSvr = num;
    for (int i = 0; i < num; i++) {
        getcmdToAe *mev = new getcmdToAe(i);
        SvrList.push_back(mev);

        pthread_t tid;
        pthread_create(&tid, NULL, Run, (void *) mev);
    }
}

getcmdToAe::getcmdToAe(int _id) {
    id = _id;
    pipeLine = new aePipe(recvn, sendn);
}

getcmdToAe::~getcmdToAe() {
}

void *getcmdToAe::Run(void *arg) {
    getcmdToAe *me = (getcmdToAe *) arg;
    me->el = aeCreateEventLoop(65535);
    me->el->timeout = 1000;
    me->el->timeOutdo = me->timeOutProc;
    me->el->t = (void *) me;
    aeCreateFileEvent(me->el, me->pipeLine->fdIn[0], AE_READABLE, me->recvn, NULL);

    std::cout << "getcmdToAe  start............." << std::endl;
    aeMain(me->el);
    aeDeleteEventLoop(me->el);

    return NULL;
}


int getcmdToAe::GetcmdtimeNOut(JOB *eTask) {
//   std::cout << "XXXXXXXXXXXXXx  in:"<<eTask->topic <<",xiyd:"<< eTask->xyid<< std::endl;
//    static long long num = 0;
//    num++;
    char tmp[300];
    sprintf(tmp, "%s_%d", eTask->topic, eTask->groupid);
    std::string topic = tmp;

    opleveldb *opdb = doCmdFindDB[topic][eTask->appid];
    if (!opdb) {
        opdb = new opleveldb(eTask->appid, eTask->groupid);
        opdb->opinitdb(topic);
        doCmdFindDB[topic][eTask->appid] = opdb;
    }

    if (eTask->xyid == 0) {//set
        opdb->opput(topic, std::string(eTask->p, eTask->slen));
        opPushlist2[topic] = opPushlist1[topic];

        //loop push
        for (std::map<std::string, std::map<int, opleveldb *> >::iterator iter = opPushlist2.begin();
             iter != opPushlist2.end();) {
            std::string data;
            std::string optopic = iter->first;
            std::map<int, opleveldb *> &Opmap = iter->second;

            for (std::map<int, opleveldb *>::iterator opiter = Opmap.begin(); opiter != Opmap.end();) {
                int consumAppid = opiter->first;
                opleveldb *opdbloop = opiter->second;

                if (opdbloop) {
                    int flag = opdbloop->opget(optopic, &data);
                    if (flag >= 0) {
                        JOB *job = new JOB(3, consumAppid, 0, optopic, (char *) data.c_str(), data.length());
                        job->t = opdbloop->t;
//                        assert(job->t != NULL);
                        pushlist.push_back(job);

                    } else {
                        Opmap.erase(opiter++);
                        continue;
                    }
                }// end if
                opiter++;
            } //for opiter

            if (Opmap.empty())
                opPushlist2.erase(iter++);
            else
                iter++;
        }//loop push end
//
        if (!pushlist.empty()) {
            aeCreateFileEvent(el, pipeLine->fdOut[1], AE_WRITABLE, sendn, NULL);
            aeDeleteFileEvent(el, pipeLine->fdIn[0], AE_READABLE);
        }



//        for (std::map<std::string, std::map<int, opleveldb *> >::iterator iter = opPushlist1.begin();
//             iter != opPushlist1.end(); iter++) {
//            std::string data;
//            std::string optopic = iter->first;
//            std::map<int, opleveldb *> &Opmap = iter->second;
//
//            for (std::map<int, opleveldb *>::iterator opiter = Opmap.begin(); opiter != Opmap.end(); opiter++) {
//                int consumAppid = opiter->first;
//                opleveldb *opdbloop = opiter->second;
//
//                if (opdbloop) {
//                    int flag = opdbloop->opget(optopic, &data);
//                    if (flag >= 0) {
//                        JOB *job = new JOB(3, consumAppid, 0, optopic, (char *) data.c_str(), data.length());
//                        job->t = opdbloop->t;
//                        assert(job->t != NULL);
//
//                        pushlist.push_back(job);
//                        aeCreateFileEvent(el, pipeLine->fdOut[1], AE_WRITABLE, sendn, NULL);
//                    }
//                }
//            }
//        }//loop push end

    } // end if set 0
    else if (eTask->xyid == 1) {//get  // add pushTable
        opleveldb *opgetdb = doCmdFindDB[topic][eTask->appid];
        if (!opgetdb) {
            opgetdb = new opleveldb(eTask->appid, eTask->groupid);
            opgetdb->opinitdb(topic, (long long) eTask->consumline);
            doCmdFindDB[topic][eTask->appid] = opgetdb;
        } else {
            opgetdb->setConsumerLine(topic, (long long) eTask->consumline);
        }
//        assert(eTask->t != NULL);
//        opgetdb->t = eTask->t;
        std::cout << "Reg consum:" << eTask->topic << ",offset:" << eTask->offset << std::endl;

        opPushlist1[topic][eTask->appid] = opgetdb;
//        opPushlist2[topic][eTask->appid] = opgetdb;

    } //if get 1

    delete eTask;

    return 0;
}

int getcmdToAe::GetcmdtimeOut() {
//    std::cout << "XXXXXXXXXXXXXx  tiomeOut" << std::endl;
//  loop push

    std::list<int> flaglist;
    flaglist.clear();
    for (std::map<std::string, std::map<int, opleveldb *> >::iterator iter = opPushlist1.begin();
         iter != opPushlist1.end(); iter++) {
        std::string data;
        std::string optopic = iter->first;
        std::map<int, opleveldb *> &Opmap = iter->second;

        for (std::map<int, opleveldb *>::iterator opiter = Opmap.begin(); opiter != Opmap.end(); opiter++) {
            int consumAppid = opiter->first;
            opleveldb *opdbloop = opiter->second;

            if (opdbloop) {
                int flag = opdbloop->opget(optopic, &data);
                if (flag >= 0) {
                    JOB *job = new JOB(3, consumAppid, 0, optopic, (char *) data.c_str(), data.length());
                    job->t = opdbloop->t;
                    assert(job->t != NULL);

                    pushlist.push_back(job);
                    aeCreateFileEvent(el, pipeLine->fdOut[1], AE_WRITABLE, sendn, NULL);

                    flaglist.push_back(1);
                }
            }
        }
    }//loop push end

    if (flaglist.empty()) setTimeOut(1000); else setTimeOut(1);

    return 0;
}