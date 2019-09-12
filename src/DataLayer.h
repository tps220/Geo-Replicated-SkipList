#ifndef DATA_LAYER_H
#define DATA_LAYER_H

#include "SearchLayer.h"
#include "Nodes.h"
#include "Hazard.h"

extern searchLayer_t** numaLayers;
extern int numberNumaZones;
typedef struct dataLayerThread_t {
  pthread_t runner;
  volatile char running;
  volatile char finished;
  int sleep_time;
  node_t* sentinel;
  HazardNode_t* hazardNode;
} dataLayerThread_t;

typedef struct pair {
  node_t* previous;
  node_t* current;
} pair_t;

//Driver Functions
int lazyFind(searchLayer_t* numask, int val, HazardNode_t* hazardNode);
int lazyAdd(searchLayer_t* numask, int val, HazardNode_t* hazardNode);
int lazyRemove(searchLayer_t* numask, int val, HazardNode_t* hazardNode);

//Background functions
void* backgroundRemoval(void* input);
void* garbageCollectDataLayer(void* input);
void startDataLayerHelpers(node_t* sentinel, HazardNode_t* hazardNode);
void stopDataLayerHelpers(HazardNode_t* hazardNode);

#endif
