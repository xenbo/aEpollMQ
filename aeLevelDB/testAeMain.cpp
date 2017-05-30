#include <iostream>
#include <cstdio>
#include <cstring>
#include <unistd.h>
#include <pthread.h>

#include "getcmdToAe.h"

using namespace std;


std::string topic1 = "topicxxxx";
std::string value1 = "ABCJHSJKFKLJKLHK";

getcmdToAe *docmd[4];
void *test(void *arg) {
    for (int j = 0; j < 100000; j++) {
        for (int i = 0; i < 4; i++) {
            char valuetmp[100];
            sprintf(valuetmp, "_%d_%d", i, j);
            char keytmp[100];
            sprintf(keytmp, "_%d", i);

            std::string key = topic1 + keytmp;
            std::string value = value1 + valuetmp;


            JOB *job = new JOB(0, 100+i, 1011, key, (char*)value.c_str(), value.length());
            unsigned long addr = (unsigned long) job;
            job->consumline =-1;
            job->t =(void*)1;
            write(docmd[i]->pipeLine->fdIn[1], (char *) &addr, sizeof(unsigned long));
        }
    }

}

int main() {

    getcmdToAe::getcmdToAeInit(4);

    for (int i = 0; i < 4; i++)
        docmd[i] = getcmdToAe::SvrList[i];


    pthread_t tid;
    pthread_create(&tid, NULL, test, NULL);

    sleep(1);

    for (int i = 0; i < 4; i++) {
        char keytmp[100];
        sprintf(keytmp, "_%d", i);
        std::string key = topic1 + keytmp;

        JOB *job = new JOB(1, 100 + i, 1011,  key, NULL ,0);
        unsigned long addr = (unsigned long) job;
        write(docmd[i]->pipeLine->fdIn[1], (char *) &addr, sizeof(unsigned long));
    }


    getchar();


    return 0;
}