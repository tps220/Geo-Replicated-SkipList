#ifndef SEARCH_LAYER_H
#define SEARCH_LAYER_H

#include "Nodes.h"
#include "JobQueue.h"
#include "MemoryQueue.h"
#include <pthread.h>

typedef struct searchLayer {
  inode_t* sentinel;
  pthread_t helper;
  job_queue_t* updates;
  pthread_t reclaimer;
  memory_queue_t* garbage;
  int numaZone;
  volatile char finished;
  volatile char stopGarbageCollection;
  volatile char running;
  int sleep_time;
} searchLayer_t;

//driver functions
searchLayer_t* constructSearchLayer(inode_t* sentinel, int zone);
searchLayer_t* destructSearchLayer(searchLayer_t* searcher);
int searchLayerSize(searchLayer_t* numask);
void startIndexLayer(searchLayer_t* numask, int sleep_time);
void stopIndexLayer(searchLayer_t* numask);

//helper functions
int runJob(inode_t* sentinel, q_node_t* job, int zone, memory_queue_t* gc);
void* updateNumaZone(void* args);
void* garbageCollectionIndexLayer(void* args);

#endif
