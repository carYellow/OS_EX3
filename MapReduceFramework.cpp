//
// Created by idan.stollman The Great on 01/06/2021.
//

#include "MapReduceFramework.h"

void *threadJob(void *arg);

int spawnThreads(JobManager *jobManager);

int initJobManager(JobManager *jobManager, int threadsNum);

void emit2(K2 *key, V2 *value, void *context) {}

void emit3(K3 *key, V3 *value, void *context) {}

JobHandle startMapReduceJob(const MapReduceClient &client,
                            const InputVec &inputVec, OutputVec &outputVec,
                            int multiThreadLevel) {
    auto *jobManager = new JobManager();
    initJobManager(jobManager, multiThreadLevel);
    spawnThreads(jobManager);



    return jobManager;
}

void waitForJob(JobHandle job) {}


void getJobState(JobHandle job, JobState *state) {}

void closeJobHandle(JobHandle job) {}

// ______________________________________________________________________


void *threadJob(void *arg) {

}

int initJobManager(JobManager *jobManager, int threadsNum) {
    jobManager->barrier = new Barrier(threadsNum);
    jobManager->ThreadsNum = threadsNum;
    jobManager->threads = new pthread_t[threadsNum];
    jobManager->threadsContexts = new ThreadContext[threadsNum];

}

int spawnThreads(JobManager *jobManager) {
    for (int i = 0; i < jobManager->ThreadsNum; ++i) {
        pthread_create((jobManager->threads) + i, NULL, threadJob, (jobManager->threadsContexts) + i);
    }

}




