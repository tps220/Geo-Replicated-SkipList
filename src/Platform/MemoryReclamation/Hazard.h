#ifndef HAZARD_H
#define HAZARD_H

#include "Allocator.h"
#include "LinkedList.h"

extern numa_allocator_t** allocators;
#define MAX_DEPTH 10

typedef struct HazardNode {
    void* hp0;
    void* hp1;
    LinkedList_t* retiredList;
    struct HazardNode* next;
    unsigned int id;
} HazardNode_t;

HazardNode_t* constructHazardNode(unsigned int zone, unsigned int id);
void destructHazardNode(HazardNode_t* node, unsigned int zone, int isDataLayer);

typedef struct HazardContainer {
    HazardNode_t* head;
} HazardContainer_t;

HazardContainer_t* constructHazardContainer(HazardNode_t* head);
void destructHazardContainer(HazardContainer_t* container);

extern HazardContainer_t* memoryLedger;

void retireElement(LinkedList_t* retiredList, void* ptr, void (*reclaimMemory)(void*, int), unsigned int zone);
void scan(LinkedList_t* retiredList, void (*reclaimMemory)(void*, int), unsigned int zone);
void reclaimIndexNode(void* ptr, int zone);
void reclaimDataLayerNode(void* ptr, int zone);

#define RETIRE_INDEX_NODE(retiredList, ptr, zone) retireElement((retiredList), (ptr), reclaimIndexNode, (zone))
#define RETIRE_NODE(hazardNode, ptr) retireElement((hazardNode -> retiredList), (ptr), reclaimDataLayerNode, 0)

#endif
