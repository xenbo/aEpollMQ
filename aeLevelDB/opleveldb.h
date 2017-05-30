//
// Created by dongbo01 on 5/11/17.
//

#ifndef LEVELDBTEST_OPLEVELDB_H
#define LEVELDBTEST_OPLEVELDB_H

#include <iostream>
#include <map>
#include <list>
#include "leveldb/db.h"

#define DBPATH  "./DBDATA/"
#define MAXLENGTH 100000


struct LDB {
    std::string dataBaseTopic;
    leveldb::DB *db;

    long long startLine;
    long long endLine;

    std::list<std::string> dataList;
    leveldb::Options options;
    leveldb::WriteOptions woptions;

    LDB(std::string Topic);

    static int Init();

    int _put(std::string key, std::string data);

    int _get(std::string, std::string *data);

    int _delete(std::string key);

    static std::map<std::string, LDB *> TopicFindDB;


    static pthread_spinlock_t DBlock;
    static pthread_once_t one_spin;

    static void  spin_init() {
        pthread_spin_init(&DBlock, 0);
    }

    static std::map<std::string, LDB *> createMap() {
        pthread_once(&one_spin, spin_init);
        std::map<std::string, LDB *> _TopicFindDB;
        return _TopicFindDB;
    }


    static LDB *getDB(std::string Topic) {
        LDB *_ldb;
        pthread_spin_lock(&DBlock);
        _ldb = LDB::TopicFindDB[Topic];
        pthread_spin_unlock(&DBlock);

        return _ldb;
    }


    std::map<std::string ,std::string> DATATEST;
};

class opleveldb {
public:
    opleveldb(int _appid, int _gropuid);

    LDB *opinitdb(std::string Topic);

    LDB *opinitdb(std::string Topic, long long consustartLine);

    int opget(std::string topic, std::string *data);

    int opput(std::string topic, std::string data);

    int setConsumerLine(std::string Topic, long long consustartLine);

    void *t;
private:
    int appid;
    int groupid;
    std::map<std::string, LDB *> TopicFindDB;
    std::map<std::string, long long> consumMapLine;
};


#endif //LEVELDBTEST_OPLEVELDB_H
