//
// Created by dongbo01 on 5/11/17.
//

#include <dirent.h>
#include <cstdlib>
#include <cassert>

#include "opleveldb.h"

pthread_spinlock_t LDB::DBlock = {0};
pthread_once_t LDB::one_spin = PTHREAD_ONCE_INIT;
std::map<std::string, LDB *> LDB::TopicFindDB = LDB::createMap();

LDB::LDB(std::string Topic) {
    pthread_spin_lock(&DBlock);

//    woptions.sync = false;
//    options.create_if_missing = true;
//    options.block_size = 1000 * 1024 * 1024;
//    std::string path = DBPATH + Topic;
//    leveldb::Status status = leveldb::DB::Open(options, path, &db);
//
//    assert(status.ok());

    startLine = endLine = 0;
    TopicFindDB[Topic] = this;
    pthread_spin_unlock(&DBlock);
}

int LDB::Init() {
    DIR *dir;
    struct dirent *ptr;

    if ((dir = opendir(DBPATH)) == NULL) {
        printf("Open dir error \n");
        return -1;
    }

    while ((ptr = readdir(dir)) != NULL) {
        if (strcmp(ptr->d_name, ".") == 0 || strcmp(ptr->d_name, "..") == 0) {
            continue;
        }
        if (ptr->d_type == 4) {
            std::string topicName = std::string(ptr->d_name);
            std::string data;

            LDB *_ldb = new LDB(topicName);
            _ldb->_get(topicName, &data);
            _ldb->startLine = atoi(data.c_str() + 1);
            _ldb->endLine = atoi(data.c_str() + data.find(":") + 1);

            for (long long i = _ldb->startLine; i < _ldb->endLine; i++) {
                char tmp[256];
                sprintf(tmp, "%s_%lld", topicName.c_str(), i);
                std::string key = tmp;
                std::cout << "have key:" << key << std::endl;
                _ldb->dataList.push_back(key);
            }

            std::cout << topicName << ",start:" << _ldb->startLine << ",end:" << _ldb->endLine << std::endl;

            LDB::TopicFindDB[topicName] = _ldb;
        }
    }

    return 0;
}

int LDB::_put(std::string key, std::string data) {
//    db->Put(leveldb::WriteOptions(), key, data);

    DATATEST[key] =data;
    return 0;
}

int LDB::_get(std::string key, std::string *data) {
//    db->Get(leveldb::ReadOptions(), key, data);
    *data = DATATEST[key];
    return 0;
}

int LDB::_delete(std::string key) {
//    db->Delete(leveldb::WriteOptions(), key);
    std::map<std::string ,std::string>::iterator iter = DATATEST.find(key);
    if(iter != DATATEST.end())
        DATATEST.erase(iter);

    return 0;
}

opleveldb::opleveldb(int _appid, int _gropuid) {
    appid = _appid;
    groupid = _gropuid;
}


LDB *opleveldb::opinitdb(std::string Topic, long long consustartLine) {
    LDB *ldb = NULL;
    ldb = LDB::getDB(Topic);
    if (ldb == NULL) ldb = new LDB(Topic);

    TopicFindDB[Topic] = ldb;

    char tmp[256];
    sprintf(tmp, "_%d:%d", appid, groupid);
    std::string keyuser = tmp;
    std::string LineData;
    ldb->_put(keyuser, LineData);

    long long &consumLine = consumMapLine[Topic];
    __sync_lock_test_and_set(&consumLine, consustartLine);

    return ldb;
}

LDB *opleveldb::opinitdb(std::string Topic) {
    LDB *ldb = NULL;
    ldb = LDB::getDB(Topic);
    if (ldb == NULL) ldb = new LDB(Topic);

    TopicFindDB[Topic] = ldb;

    char tmp[256];
    sprintf(tmp, "_%d:%d", appid, groupid);
    std::string keyuser = tmp;
    std::string LineData;
    ldb->_get(keyuser, &LineData);

    long long &consumLine = consumMapLine[Topic];
    __sync_lock_test_and_set(&consumLine, atoi(LineData.c_str() + 1));

    return ldb;
}

int opleveldb::opput(std::string topic, std::string data) {
    LDB *ldb = TopicFindDB[topic];
    if (ldb == NULL) {
        ldb = opinitdb(topic);
    }

    long long sline = __sync_fetch_and_add(&ldb->startLine, 0);
    long long eline = __sync_fetch_and_add(&ldb->endLine, 1);

    char tmp[256];
    sprintf(tmp, "%s_%lld", topic.c_str(), eline);
    std::string key = tmp;

    ldb->_put(key, data);
    ldb->dataList.push_back(key);
    //std::cout << "Put:" << key <<" : "<< data << std::endl ;

    while (ldb->dataList.size() >= MAXLENGTH) {
        sline = __sync_fetch_and_add(&ldb->startLine, 1);
        sprintf(tmp, "%s_%lld", topic.c_str(), sline);
        std::string key2 = tmp;
        ldb->_delete(key2);
        ldb->dataList.pop_front();
        std::cout << "Delete:" << key2 << ",size:" << ldb->dataList.size() << ":" << MAXLENGTH << std::endl;
    }

    //std::cout << "size: "<<ldb->dataList.size() << std::endl;

    sprintf(tmp, "_%lld:%lld", sline, eline);
    std::string LineData = tmp;
    ldb->_put(topic, LineData);    //ldb->endLine ldb->statrLine into file

    return 0;
}

int opleveldb::opget(std::string topic, std::string *data) {
    LDB *ldb = TopicFindDB[topic];
    if (ldb == NULL) {
        ldb = opinitdb(topic);
    }

    long long &consumLine = consumMapLine[topic];
    long long line = __sync_fetch_and_add(&consumLine, 0);
    if (line >= __sync_fetch_and_add(&ldb->endLine, 0)) {
//        std::cout << "not find:" << topic << " : " << *data << ",consline:" << line << std::endl;
        return -1;
    }
    __sync_fetch_and_add(&consumLine, 1);

    long long datastartLine = __sync_fetch_and_add(&ldb->startLine, 0);
    if (line < datastartLine) {

        __sync_lock_test_and_set(&consumLine, datastartLine + 1);
        line = datastartLine;
    }

    char tmp[256];
    sprintf(tmp, "%s_%lld", topic.c_str(), line);
    std::string key = tmp;

    ldb->_get(key, data);
    // std::cout << "Get:" << key  << ",consline:" << line << std::endl;

    sprintf(tmp, "%s_%d:%d", topic.c_str(), appid, groupid);
    std::string keyuser = tmp;
    sprintf(tmp, "_%lld", line);
    std::string LineData = tmp;
    ldb->_put(keyuser, LineData);    //consumLine write into file

    return 0;
}

int opleveldb::setConsumerLine(std::string Topic, long long consustartLine) {
    LDB *ldb = NULL;
    ldb = TopicFindDB[Topic];

    assert(ldb != NULL);

    char tmp[256];
    sprintf(tmp, "%s_%d:%d", Topic.c_str(), appid, groupid);
    std::string keyuser = tmp;
    long long &consumLine = consumMapLine[Topic];

    std::string LineData;
    if (consustartLine >= 0) {
        consumLine = consustartLine;
        char tmp[256];
        sprintf(tmp, "_%lld", consumLine);
        LineData = tmp;
        ldb->_put(keyuser, LineData);
        __sync_lock_test_and_set(&consumLine, consustartLine);
    } else if (consustartLine == -1) {
        ldb->_get(keyuser, &LineData);
        __sync_lock_test_and_set(&consumLine, atoi(LineData.c_str() + 1));
    } else if (consustartLine == -2) {
        __sync_lock_test_and_set(&consumLine, ldb->endLine);
    }


    return 0;
}
