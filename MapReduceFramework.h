#ifndef MAPREDUCEFRAMEWORK_H
#define MAPREDUCEFRAMEWORK_H

#include "MapReduceClient.h"
#include "Barrier/Barrier.h"
#include <atomic>

typedef void *JobHandle;
struct JobManager;
enum stage_t {
    UNDEFINED_STAGE = 0, MAP_STAGE = 1, SHUFFLE_STAGE = 2, REDUCE_STAGE = 3
};

typedef void (*clientMapFunc)(const K1 *, const V1 *, void *);

typedef void (*clientReduceFunc)(const IntermediateVec *, void *);

typedef struct {
    IntermediateVec intermediateVec;
    JobManager *jobManager;
    int tid;

}ThreadContext;

struct JobManager {
    //JobManager(const MapReduceClient &mapReduceClient);

    Barrier *barrier;
    pthread_t *threads;
    ThreadContext *threadsContexts;
    int ThreadsNum;
    std::atomic<int> nextPairIdx;
    OutputVec outputVec;
    InputVec inputVec;
    clientMapFunc mapFunc;
    clientReduceFunc reduceFunc;
    const MapReduceClient &mapReduceClient;

    JobManager(int threadsNum, const MapReduceClient &mapReduceClient)
            : mapReduceClient(mapReduceClient) {

        barrier = new Barrier(threadsNum);
        ThreadsNum = threadsNum;
        threads = new pthread_t[threadsNum];
        threadsContexts = new ThreadContext[threadsNum];
        nextPairIdx = 0;

        for (int i = 0; i < ThreadsNum; ++i) {
            threadsContexts[i].jobManager = this;
            threadsContexts[i].tid = i;
        }

    }

};

typedef struct {
    stage_t stage;
    float percentage;
} JobState;

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
