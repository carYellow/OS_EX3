//
// Created by idan.stollman The Great on 01/06/2021.
//

#include <algorithm>
#include "MapReduceFramework.h"


void *threadFunc(void *arg);

int spawnThreads(JobManager *jobManager);

//int initJobManager(JobManager *jobManager, int threadsNum,  const MapReduceClient &client);

std::vector<IntermediateVec>* shuffle(ThreadContext *tc);

void emit2(K2 *key, V2 *value, void *context) {

    auto *tc = (ThreadContext*)context;
    IntermediatePair intermediatePair(key,value);
    tc->intermediateVec.push_back(intermediatePair);
}

void emit3(K3 *key, V3 *value, void *context) {

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
    JobManager *jobManager = new JobManager(multiThreadLevel, client);

//    initJobManager(jobManager, multiThreadLevel, client);
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
    //TODO: This line does not run but we are the greatest and Idan our amazing Team leader has instructed us to move forward and his wish is our command
    tc->jobManager->mapReduceClient.map(tc->jobManager->inputVec[oldValue].first,
                                         tc->jobManager->inputVec[oldValue].second, tc);

    //sortPhase();
    std::sort(tc->intermediateVec.begin(),tc->intermediateVec.end());

    tc->jobManager->barrier->barrier();
    if(tc->tid == 0){
        std::vector<IntermediateVec>shuffledVector = shuffle(tc);
    }
    //They said we should use a se,ephore fr this stage instaed of a barriar
    //reset barrier

    tc->jobManager->barrier->barrier();





    //barrier;
    //shuffle; only thread 0.
//    reducePhase();
    return nullptr;
}
/**
 * Checks if all the intermedate vectprs are empty
 * @param tc
 * @return true if not all the vectors are empty else false
 */
bool notAllTheIntermediateVectorsAreEmpty(ThreadContext *tc){
    for (int i = 0; i < tc->jobManager->ThreadsNum; i++){
        if(tc->jobManager->threadsContexts[i].intermediateVec.size() > 0){
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
std::vector<int> getIdsOfThreadsWithLargestKeys(ThreadContext *tc){
    std::vector<int>  idsOfThreadsWithLargestKeys =  std::vector<int>();
    auto largestPair = tc->jobManager->threadsContexts[0].intermediateVec.back(); //TPDO make sure this is the right type

    //Finding the largest Key
    for(int i = 0; i < tc->jobManager->ThreadsNum; i++){
        // Get id's of threads with largest keys
        auto currentPair = tc->jobManager->threadsContexts[i].intermediateVec.back();
        if(currentPair > largestPair){
            largestPair = currentPair;
        }
    }

    //Finding all the vectors that have the largest keys.
    for(int i = 0; i < tc->jobManager->ThreadsNum; i++){
        auto currentPair = tc->jobManager->threadsContexts[i].intermediateVec.back();
        //Check if this vector also has the largest index
        if(!(largestPair > currentPair) && (largestPair < currentPair)){
            idsOfThreadsWithLargestKeys.push_back(i);
        }
    }
    return idsOfThreadsWithLargestKeys;

}
std::vector<IntermediateVec>* shuffle(ThreadContext *tc) {
    auto* shuffledVec = new std::vector<IntermediateVec>();

    while(notAllTheIntermediateVectorsAreEmpty(tc)){
        std::vector<int> idsOfThreadsWithLargestKeys = getIdsOfThreadsWithLargestKeys(tc);
        std::vector<std::pair<K2 *, V2 *>> vectorOfLargestPairs =   std::vector<std::pair<K2 *, V2 *>>();
        for (int j = 0; j < idsOfThreadsWithLargestKeys.size(); ++j) {
            //Get the largest pair from the vector
            IntermediatePair largest_pair = tc->intermediateVec.back();
            //pop largest pair from the vector
            tc->intermediateVec.pop_back();
            //Add pair to the the new vector
            vectorOfLargestPairs.push_back(largest_pair);

        }
        shuffledVec->push_back(vectorOfLargestPairs);
    }

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
