//
// Created by idan.stollman The Great on 01/06/2021.
//

#include <algorithm>
#include <iostream>
#include "MapReduceFramework.h"
#include "Barrier.h"
#include <atomic>


typedef struct ThreadContext {
    IntermediateVec intermediateVec;
    JobManager *jobManager;
    int tid;

} ThreadContext;

struct JobManager {
    //JobManager(const MapReduceClient &mapReduceClient);

    Barrier *sortBarrier;
    Barrier *shuffleBarrier;
//    std::mutex *reduceMutex; // no no
//    std::mutex *outputMutex; // no no todo
    pthread_mutex_t  percentageMutex;
    pthread_mutex_t  reduceMutex;
    pthread_mutex_t  statusMutex;

    pthread_t *threads;
    ThreadContext *threadsContexts;
    int ThreadsNum;
    std::atomic<u_int64_t> atomicCounter;
    std::atomic<u_int64_t> pairsFinished;
    OutputVec& outputVec;
    InputVec inputVec;
    std::vector<IntermediateVec *> *shuffledVector;
//    clientMapFunc mapFunc;
//    clientReduceFunc reduceFunc;
    const MapReduceClient &mapReduceClient;
    JobState *jobState;
    std::atomic<int> stage;
    std::atomic<int> totalWork;
    std::atomic<int> numberOfVector;
    std::atomic<int> numberOfIntermediatePairs;
    bool joinedWasCalled = false;



    JobManager(int threadsNum, const MapReduceClient &mapReduceClient, const InputVec &inputVec, OutputVec &outputVec)
            : mapReduceClient(mapReduceClient), inputVec(inputVec), outputVec(outputVec) {
//            : mapReduceClient(mapReduceClient) ,outputVec(outputVec){
//    {

        sortBarrier = new Barrier(threadsNum);
        shuffleBarrier = new Barrier(threadsNum);
        reduceMutex = PTHREAD_MUTEX_INITIALIZER;
        percentageMutex = PTHREAD_MUTEX_INITIALIZER;
        statusMutex = PTHREAD_MUTEX_INITIALIZER;
        numberOfIntermediatePairs = 0;
        ThreadsNum = threadsNum;
        threads = new pthread_t[threadsNum];
        threadsContexts = new ThreadContext[threadsNum];
        atomicCounter = 0;
        pairsFinished = 0;
        stage = 0;
        numberOfVector=0;
        totalWork = inputVec.size();

        /*for (int i = 0; i < threadsNum; ++i) {
            threadsContexts[i].jobManager = this;
            threadsContexts[i].tid = i;
        }*/
        jobState = new JobState();
        jobState->stage = UNDEFINED_STAGE;

    }


};

void *threadFunc(void *arg);

int spawnThreads(JobManager *jobManager);

//int initJobManager(JobManager *jobManager, int threadsNum,  const MapReduceClient &client);

std::vector<IntermediateVec *> *shuffle(ThreadContext *);

void emit2(K2 *key, V2 *value, void *context) {

    auto *tc = (ThreadContext *) context;
    IntermediatePair intermediatePair(key, value);
    tc->intermediateVec.push_back(intermediatePair);
}

void emit3(K3 *key, V3 *value, void *context) {
    ThreadContext *tc = (ThreadContext *) context;
    pthread_mutex_lock(&tc->jobManager->reduceMutex);
//    tc->jobManager->outputMutex->lock();// no no todo

    //critical sec start--------------------------------
//    OutputVec o
    OutputPair outputPair(key, value);

    tc->jobManager->outputVec.push_back(outputPair);
//    tc->jobManager->atomicCounter++;
    tc->jobManager->pairsFinished++;
    //critical sec end---------------------------------
//    tc->jobManager->outputMutex->unlock();
    pthread_mutex_unlock(&tc->jobManager->reduceMutex);
}


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
    auto *jobManager = new JobManager(multiThreadLevel, client, inputVec, outputVec);
    for (int i = 0; i < multiThreadLevel; ++i) {
        jobManager->threadsContexts[i].jobManager = jobManager;
        jobManager->threadsContexts[i].tid = i;
    }

//    initJobManager(jobManager, multiThreadLevel, client);
    int res = spawnThreads(jobManager);
    if(res <0){
        fprintf(stderr, "system error: Could Not Create Thread, maybe be use std threads it they are better then pthreads\\n");
        exit(1);
    }

    return jobManager;
}

/**
 * Function gets JobHandle returned by startMapReduceFrameworkand waits until it is finished.
 * @param job
 */
void waitForJob(JobHandle job) {
    //It is legal to call the function more than once and you should handle it.
    // Pay attention that calling pthread_join twice from the same process has undefined behaviorand you must avoid that.
    auto *jobManager = (JobManager *) job;
    if(jobManager->joinedWasCalled){
        return;
    }
    jobManager->joinedWasCalled = true;
    for (int i = 0; i < jobManager->ThreadsNum; ++i) {
        pthread_join(jobManager->threads[i], nullptr);
    }


}


void getJobState(JobHandle job, JobState *state) {
    JobManager *jobManager = (JobManager *) job;
//    stage_t stage = (stage_t) jobManager->stage.load();
    pthread_mutex_lock(&jobManager->statusMutex);
    state->stage = (stage_t)jobManager->stage.load();
    state->percentage = ((float) jobManager->pairsFinished)*100 / jobManager->totalWork;
    pthread_mutex_unlock(&jobManager->statusMutex);
}
/**
 * Releasing all resources of a job. You should prevent releasing resources before the job finished. After this function
 * is called the job handle will be invalid.In case that the functioni s called and the
 * job is not finished yet wait until the job is finished to close it
 * @param job
 */
void closeJobHandle(JobHandle job) {
    waitForJob(job);
    auto * jobManager = (JobManager * )job;
    delete jobManager->shuffleBarrier;
    delete jobManager->sortBarrier;
    delete jobManager->jobState;
    delete [] jobManager->threads;
    delete [] jobManager->threadsContexts;
    pthread_mutex_destroy(&jobManager->reduceMutex);
    for (int i = 0; i < jobManager->shuffledVector->size(); ++i) {
        delete jobManager->shuffledVector->at(i);
    }
    delete jobManager->shuffledVector;
    pthread_mutex_destroy(&jobManager->percentageMutex);
    pthread_mutex_destroy(&jobManager->statusMutex);
    delete jobManager;
}

// ______________________________________________________________________


void *threadFunc(void *arg) {
//    mapPhase();

    ThreadContext *tc = (ThreadContext *) arg;

    int oldValue = tc->jobManager->atomicCounter++;
    while (oldValue < tc->jobManager->inputVec.size()) {
        tc->jobManager->stage = MAP_STAGE;
        tc->jobManager->mapReduceClient.map(tc->jobManager->inputVec[oldValue].first,
                                            tc->jobManager->inputVec[oldValue].second, tc);

        oldValue = tc->jobManager->atomicCounter++;
        // todo mutex
        pthread_mutex_lock(&tc->jobManager->statusMutex);
        tc->jobManager->pairsFinished++;
        pthread_mutex_unlock(&tc->jobManager->statusMutex);
        // insert in JOB STATE
    }

//    tc->jobManager->totalWork=0;
    //sortPhase();
    if (!tc->intermediateVec.empty()) {

//        std::sort(tc->intermediateVec.begin(), tc->intermediateVec.end());
        pthread_mutex_lock(&tc->jobManager->percentageMutex);
        int old = tc->jobManager->numberOfIntermediatePairs.load();
        tc->jobManager->numberOfIntermediatePairs = old +tc->intermediateVec.size();
        pthread_mutex_unlock(&tc->jobManager->percentageMutex);
//        int old = tc->jobManager->numberOfIntermediatePairs.load();
//        tc->jobManager->numberOfIntermediatePairs = old +tc->intermediateVec.size();
        std::sort(tc->intermediateVec.begin(),tc->intermediateVec.end(),[](const IntermediatePair & left, const IntermediatePair& right) -> bool {
            return  *(left.first) < *(right.first);
        });
    }
    tc->jobManager->sortBarrier->barrier();//---------------------------------------------------
    if (tc->tid == 0) {
//        tc->jobManager->pairsFinished = 0;
        // todo
//        pthread_mutex_lock(&tc->jobManager->percentageMutex);
        pthread_mutex_lock(&tc->jobManager->statusMutex);
        tc->jobManager->stage = SHUFFLE_STAGE;
        tc->jobManager->pairsFinished = 0;
        tc->jobManager->totalWork = tc->jobManager->numberOfIntermediatePairs.load();
        pthread_mutex_unlock(&tc->jobManager->statusMutex);
//        pthread_mutex_unlock(&tc->jobManager->percentageMutex);
//        printf("%d\n", tc->jobManager->totalWork.load());
//        fflush(stdout);

//        tc->jobManager->stage = SHUFFLE_STAGE;
        tc->jobManager->shuffledVector = shuffle(tc);
        tc->jobManager->atomicCounter=0;
        tc->jobManager->numberOfVector = 0;
//        tc->jobManager->pairsFinished =0;

        pthread_mutex_lock(&tc->jobManager->statusMutex);
        tc->jobManager->stage = REDUCE_STAGE;
        tc->jobManager->pairsFinished =0;
        pthread_mutex_unlock(&tc->jobManager->statusMutex);

        // todo
//        pthread_mutex_lock(&tc->jobManager->percentageMutex);
        tc->jobManager->totalWork = tc->jobManager->shuffledVector->size();
//        pthread_mutex_unlock(&tc->jobManager->percentageMutex);

    }
    //They said we should use a semephore fr this stage instaed of a barriar
    //reset barrier

    tc->jobManager->shuffleBarrier->barrier();//--------------------------------------------------

    int oldValTwo =  tc->jobManager->numberOfVector++;
    while (oldValTwo < tc->jobManager->shuffledVector->size()) {


//        tc->jobManager->reduceMutex->lock();
        //critical sec--------------------------------------------------------------------------------

        IntermediateVec *intermediateVec = tc->jobManager->shuffledVector->at(oldValTwo);
//        tc->jobManager->shuffledVector->pop_back();
        tc->jobManager->mapReduceClient.reduce(intermediateVec, tc);
        oldValTwo = tc->jobManager->numberOfVector++;
        //critical sec--------------------------------------------------------------------------------

//        tc->jobManager->reduceMutex->unlock();
    }
    return arg;
}

/**
 * Checks if all the intermedate vectprs are empty
 * @param tc
 * @return true if not all the vectors are empty else false
 */
bool notAllTheIntermediateVectorsAreEmpty(ThreadContext *tc) {
    for (int i = 0; i < tc->jobManager->ThreadsNum; i++) {
        if (!tc->jobManager->threadsContexts[i].intermediateVec.empty()) {
            return true;
        }
    }
    return false;
}

/**
 * retunrs indecies of vectors with largets keys
 * @param tc
 * @return indecies of vectors with largets keys
 */
std::vector<int> getIdsOfThreadsWithLargestKeys(ThreadContext *tc) {
    std::vector<int> idsOfThreadsWithLargestKeys = std::vector<int>();
    IntermediatePair largestPair;
    for (int i = 0; i < tc->jobManager->ThreadsNum; ++i) {
        if (!tc->jobManager->threadsContexts[i].intermediateVec.empty()) {
            largestPair = tc->jobManager->threadsContexts[i].intermediateVec.back();
            break;
        }
    }

    //Finding the largest Key
    for(int i = 0; i < tc->jobManager->ThreadsNum; i++){
        // Get id's of threads with largest keys
        if (tc->jobManager->threadsContexts[i].intermediateVec.empty()) {
            continue;
        }
        auto currentPair = tc->jobManager->threadsContexts[i].intermediateVec.back();
        if(*(largestPair.first) < *(currentPair.first)){
            largestPair = currentPair;
        }
    }

    //Finding all the vectors that have the largest keys.
    for (int i = 0; i < tc->jobManager->ThreadsNum; i++) {
        if (tc->jobManager->threadsContexts[i].intermediateVec.empty()) {
            continue;
        }
        auto currentPair = tc->jobManager->threadsContexts[i].intermediateVec.back();
        //Check if this vector also has the largest index
        if(!(*(largestPair.first) < *(currentPair.first)) && !(*(currentPair.first) < *(largestPair.first))){
            idsOfThreadsWithLargestKeys.push_back(i);
        }
    }
    return idsOfThreadsWithLargestKeys;

}

std::vector<IntermediateVec *> *shuffle(ThreadContext *tc) {
    auto *shuffledVec = new std::vector<IntermediateVec *>();
    tc->jobManager->atomicCounter=0;

    while (notAllTheIntermediateVectorsAreEmpty(tc)) {
        std::vector<int> idsOfThreadsWithLargestKeys = getIdsOfThreadsWithLargestKeys(tc);
        auto *vectorOfLargestPairs =  new  std::vector<std::pair<K2 *, V2 *>>();



        for (int idOfThread : idsOfThreadsWithLargestKeys) {

            long size = tc->jobManager->threadsContexts[idOfThread].intermediateVec.size();
            for (int i = 0; i < size; ++i) {

                //Get the largest pairs from the vector
                IntermediatePair largestPair = tc->jobManager->threadsContexts[idOfThread].intermediateVec.back();
                //pop largest pair from the vector
                tc->jobManager->threadsContexts[idOfThread].intermediateVec.pop_back();
                //Add pair to the the new vector
                vectorOfLargestPairs->push_back(largestPair);
//                tc->jobManager->atomicCounter++;
                tc->jobManager->pairsFinished++;

                if(!(tc->jobManager->threadsContexts[idOfThread].intermediateVec.empty())) {
                    IntermediatePair nextPair = tc->jobManager->threadsContexts[idOfThread].intermediateVec.back();
                    if ((*(largestPair.first) < *(nextPair.first)) || (*(nextPair.first) < *(largestPair.first))) {
                        //pairs are not equal so you can continue to the next vector
                        break;
                    }
                } else{
                    break;
                }
            }
        }

        shuffledVec->push_back(vectorOfLargestPairs);
        //Whenever a new vector is inserted to the queue you shouldupdate the atomic counter.
//        tc->jobManager->atomicCounter++;
//        std::cout << (tc->jobManager->atomicCounter);
    }
//    printf("%lu\n", tc->jobManager->atomicCounter.load());
//    fflush(stdout);
    return shuffledVec;

}

//int initJobManager(JobManager *jobManager, int threadsNum, const MapReduceClient &client) {
//    jobManager->barrier = new Barrier(threadsNum);
//    jobManager->ThreadsNum = threadsNum;
//    jobManager->threads = new pthread_t[threadsNum];
//    jobManager->threadsContexts = new ThreadContext[threadsNum];
//    jobManager->nextPairIdx = 0;
//
////    jobManager->mapReduceClient = client;
////    jobManager->mapFunc = mapFunc;
////    jobManager->reduceFunc = reduceFunc;
//
//    for (int i = 0; i < jobManager->ThreadsNum; ++i) {
//        jobManager->threadsContexts[i].jobManager = jobManager;
//
//    }
//    return NULL;
//}

int spawnThreads(JobManager *jobManager) {
    for (int i = 0; i < jobManager->ThreadsNum; ++i) {
        int res = pthread_create((jobManager->threads) + i, nullptr, threadFunc, (jobManager->threadsContexts) + i);
        if(res < 0 ){
            return -1;
        }
    }
    return 0; //TODO change this
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
