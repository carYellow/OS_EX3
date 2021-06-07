#ifndef MAPREDUCEFRAMEWORK_H
#define MAPREDUCEFRAMEWORK_H

#include "MapReduceClient.h"



typedef void *JobHandle;
struct JobManager;
enum stage_t {
    UNDEFINED_STAGE = 0, MAP_STAGE = 1, SHUFFLE_STAGE = 2, REDUCE_STAGE = 3
};

typedef void (*clientMapFunc)(const K1 *, const V1 *, void *);

typedef struct {
    stage_t stage;
    float percentage;
} JobState;

typedef void (*clientReduceFunc)(const IntermediateVec *, void *);
//
//typedef struct ThreadContext {
//    IntermediateVec intermediateVec;
//    JobManager *jobManager;
//    int tid;
//
//} ThreadContext;
//
//struct JobManager {
//    //JobManager(const MapReduceClient &mapReduceClient);
//
//    Barrier *sortBarrier;
//    Barrier *shuffleBarrier;
////    std::mutex *reduceMutex; // no no
//    std::mutex *outputMutex; // no no todo
//    pthread_mutex_t  percentageMutex;
//    pthread_mutex_t  reduceMutex;
//
//    pthread_t *threads;
//    ThreadContext *threadsContexts;
//    int ThreadsNum;
//    std::atomic<u_int64_t> atomicCounter;
//    std::atomic<u_int64_t> pairsFinished;
//    OutputVec& outputVec;
//    InputVec inputVec;
//    std::vector<IntermediateVec *> *shuffledVector;
////    clientMapFunc mapFunc;
////    clientReduceFunc reduceFunc;
//    const MapReduceClient &mapReduceClient;
//    JobState *jobState;
//    std::atomic<int> stage;
//    std::atomic<int> totalWork;
//    std::atomic<int> numberOfVector;
//    std::atomic<int> numberOfIntermediatePairs;
//    bool joinedWasCalled = false;
//
//
//    JobManager(int threadsNum, const MapReduceClient &mapReduceClient, const InputVec &inputVec, OutputVec &outputVec)
//            : mapReduceClient(mapReduceClient), inputVec(inputVec), outputVec(outputVec) {
////            : mapReduceClient(mapReduceClient) ,outputVec(outputVec){
////    {
//
//        sortBarrier = new Barrier(threadsNum);
//        shuffleBarrier = new Barrier(threadsNum);
//        reduceMutex = PTHREAD_MUTEX_INITIALIZER;
//        percentageMutex = PTHREAD_MUTEX_INITIALIZER;
////        outputMutex = new std::mutex();
//        numberOfIntermediatePairs = 0;
//        ThreadsNum = threadsNum;
//        threads = new pthread_t[threadsNum];
//        threadsContexts = new ThreadContext[threadsNum];
//        atomicCounter = 0;
//        pairsFinished = 0;
//        stage = 0;
//        numberOfVector=0;
//        totalWork = inputVec.size();
//
//        /*for (int i = 0; i < threadsNum; ++i) {
//            threadsContexts[i].jobManager = this;
//            threadsContexts[i].tid = i;
//        }*/
//        jobState = new JobState();
//        jobState->stage = UNDEFINED_STAGE;
//
//    }
//
//
//};


void emit2(K2 *key, V2 *value, void *context);

void emit3(K3 *key, V3 *value, void *context);

JobHandle startMapReduceJob(const MapReduceClient &client,
                            const InputVec &inputVec, OutputVec &outputVec,
                            int multiThreadLevel);

void waitForJob(JobHandle job);

void getJobState(JobHandle job, JobState *state);

void closeJobHandle(JobHandle job);

int initJobManager(JobManager *jobManager, int threadsNum, const MapReduceClient &client);
//JobManager(int threadsNum, const MapReduceClient &client);
#endif //MAPREDUCEFRAMEWORK_H
