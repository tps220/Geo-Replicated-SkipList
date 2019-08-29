#ifndef SET_H
#define SET_H

#include "DataLayer.h"
#include "SearchLayer.h"
#include "Allocator.h"
#include "Hazard.h"

searchLayer_t** numaLayers;
numa_allocator_t** allocators;
int numberNumaZones;
unsigned int levelmax;
HazardContainer_t* memoryLedger;

int sl_contains(searchLayer_t* numask, int val, HazardNode_t* hazardNode);
int sl_add(searchLayer_t* numask, int val, HazardNode_t* hazardNode);
int sl_remove(searchLayer_t* numask, int val, HazardNode_t* hazardNode);
int sl_size(node_t* sentinel);
void sl_destruct(node_t* sentinel);

#endif
