#ifndef DATA_LAYER_H
#define DATA_LAYER_H

#include "SearchLayer.h"
#include "Nodes.h"

extern searchLayer_t** numaLayers;
extern int numberNumaZones;
extern gc_t* gc;

typedef struct dataLayerThread_t {
  pthread_t runner;
  volatile char running;
  volatile char finished;
  int sleep_time;
  node_t* sentinel;
} dataLayerThread_t;

typedef struct pair {
  node_t* previous;
  node_t* current;
} pair_t;

//Driver Functions
int lazyFind(searchLayer_t* numask, int val);
int lazyAdd(searchLayer_t* numask, int val);
int lazyRemove(searchLayer_t* numask, int val);
int rangeQuery(searchLayer_t* numask, const int lo, const int hi, int* result);

//Background functions
void* backgroundRemoval(void* input);
void* garbageCollectDataLayer(void* input);
void startDataLayerHelpers(node_t* sentinel);
void stopDataLayerHelpers();

#endif
