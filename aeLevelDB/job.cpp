//
// Created by dongbo01 on 5/12/17.
//

#include "job.h"

long JOB::num = 0;

JOB::~JOB()
{
    __sync_sub_and_fetch(&num, 1);
    if (masterp)
        __sync_sub_and_fetch(masterp, 1);
    //std::cout << "##### ~job:" << topic << std::endl;
}

JOB::JOB()
{
    memset(this, 0, sizeof(*this));
    __sync_add_and_fetch(&num, 1);
}

JOB::JOB(void *t)
{
    memset(this, 0, sizeof(*this));

    if (t != NULL)
    {
       // CIOSocket *socket = static_cast<CIOSocket *>(t);
      //  socket->Retain();
    }

    this->t = t;
    __sync_add_and_fetch(&num, 1);
}


JOB::JOB(JOB *job)
{
    memset(this, 0, sizeof(*this));

    memcpy(topic, job->topic, 256);
    //sprintf(topic, "%s", job->topic);
    groupid = job->groupid;
    consumline = job->consumline;
    Consump = job->Consump;
    t = job->t;
    offset = job->offset;
    appid = job->appid;
    topicid = job->topicid;

    //pStarline =job->pStarline ;
    //pEendline = job->pEendline ;
    pDStarline = job->pDStarline;
    pDEendline = job->pDEendline;
    xyid = job->xyid;
    isuse = job->isuse;
    fd = job->fd;
    __sync_add_and_fetch(&num, 1);
}

JOB::JOB(int _xyid, int _appid, int _groupid, std::string _topic, char *data,int datalen)
{
    memset(this, 0, sizeof(*this));
    xyid = _xyid;
    appid = _appid;
    groupid = _groupid;
    memcpy(topic, _topic.c_str(), _topic.length());
    memcpy(p, data,datalen);
    slen = datalen;
}