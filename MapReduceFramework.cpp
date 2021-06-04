//
// Created by idan.stollman The Great on 01/06/2021.
//

#include "MapReduceFramework.h"


void *threadFunc(void *arg);

int spawnThreads(JobManager *jobManager);

//int initJobManager(JobManager *jobManager, int threadsNum,  const MapReduceClient &client);

void emit2(K2 *key, V2 *value, void *context) {}

void emit3(K3 *key, V3 *value, void *context) {}


/**
 * his function starts running the MapReduce algorithm (with several threads) and returns a JobHandle. JobHandle
 * startMapReduceJob(const MapReduceClient& client, const InputVec& inputVec, OutputVec& outputVec, int multiThreadLevel)
 * ;client –The implementation of MapReduceClientor in other words the task that the framework should run.
 * inputVec –a vector of type std::vector<std::pair<K1*, V1*>>, the input elements.outputVec –a vector of
 * type std::vector<std::pair<K3*, V3*>>, to which the output elements will be added before returning.
 * You can assume that outputVec is empty 4multiThreadLevel –the number of worker threads to be used for running the
 * algorithm. You will have to create threads usingc functionpthread_create. You can assume multiThreadLevel argument
 * is valid (greater or equal to 1).Returns -The function returns JobHandle that will be used for monitoring the job.
 * You can assume that the input to this function is valid.
 * @param jobManager
 * @param threadsNum
 * @return
 */
JobHandle startMapReduceJob(const MapReduceClient &client,
                            const InputVec &inputVec, OutputVec &outputVec,
                            int multiThreadLevel) {
    JobManager *jobManager = new JobManager(multiThreadLevel, client);

    initJobManager(jobManager, multiThreadLevel, client);
    spawnThreads(jobManager);


    return jobManager;
}

/**
 * Function gets JobHandle returned by startMapReduceFrameworkand waits until it is finished.
 * @param job
 */
void waitForJob(JobHandle job) {
    //It is legal to call the function more than once and you should handle it.
    // Pay attention that calling pthread_jointwice from the same processhas undefinedbehaviorand you must avoid that.



}


void getJobState(JobHandle job, JobState *state) {}

void closeJobHandle(JobHandle job) {}

// ______________________________________________________________________


void *threadFunc(void *arg) {
//    mapPhase();

    auto *tc = (ThreadContext *) arg;

    int oldValue = tc->jobManager->nextPairIdx++;
    tc->jobManager->mapReduceClient.map(tc->jobManager->inputVec[oldValue].first,
                                         tc->jobManager->inputVec[oldValue].second, tc);
    //sortPhase();
    //barrier;
    //shuffle; only thread 0.
//    reducePhase();
    return nullptr;
}

int initJobManager(JobManager *jobManager, int threadsNum, const MapReduceClient &client) {
    jobManager->barrier = new Barrier(threadsNum);
    jobManager->ThreadsNum = threadsNum;
    jobManager->threads = new pthread_t[threadsNum];
    jobManager->threadsContexts = new ThreadContext[threadsNum];
    jobManager->nextPairIdx = 0;

//    jobManager->mapReduceClient = client;
//    jobManager->mapFunc = mapFunc;
//    jobManager->reduceFunc = reduceFunc;

    for (int i = 0; i < jobManager->ThreadsNum; ++i) {
        jobManager->threadsContexts[i].jobManager = jobManager;

    }
    return NULL;
}

int spawnThreads(JobManager *jobManager) {
    for (int i = 0; i < jobManager->ThreadsNum; ++i) {
        pthread_create((jobManager->threads) + i, nullptr, threadFunc, (jobManager->threadsContexts) + i);
    }
    return NULL;
}


//JobManager::JobManager(int threadsNum, const MapReduceClient &client, const MapReduceClient &mapReduceClient) {
//    jobManager->barrier = new Barrier(threadsNum);
//    jobManager->ThreadsNum = threadsNum;
//    jobManager->threads = new pthread_t[threadsNum];
//    jobManager->threadsContexts = new ThreadContext[threadsNum];
//    jobManager->nextPairIdx = 0;
//
//    jobManager->mapReduceClient = client;
////    jobManager->mapFunc = mapFunc;
////    jobManager->reduceFunc = reduceFunc;
//
//    for (int i = 0; i < jobManager->ThreadsNum; ++i) {
//        jobManager->threadsContexts[i].jobManager = jobManager;
//
//    }
//    return NULL;
//}
