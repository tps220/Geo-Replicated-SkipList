#ifndef HAZARD_H
#define HAZARD_H

#include "Allocator.h"
#include "LinkedList.h"

extern numa_allocator_t** allocators;
#define MAX_DEPTH 5

typedef struct HazardNode {
    void* hp0;
    void* hp1;
    struct HazardNode* next;
} HazardNode_t;

HazardNode_t* constructHazardNode(int zone);
void destructHazardNode(HazardNode_t* node, int zone);

typedef struct HazardContainer {
    HazardNode_t* head;
} HazardContainer_t;

HazardContainer_t* constructHazardContainer(HazardNode_t* head);
void destructHazardContainer(HazardContainer_t* container);

extern HazardContainer_t* memoryLedger;

void retireElement(LinkedList_t* hazardNode, void* ptr, void (*reclaimMemory)(void*, int), int zone);
void scan(LinkedList_t* hazardNode, void (*reclaimMemory)(void*, int), int zone);
void reclaimIndexNode(void* ptr, int zone);
void reclaimDataLayerNode(void* ptr, int zone);

#define RETIRE_INDEX_NODE(retiredList, ptr, zone) retireElement((retiredList), (ptr), reclaimIndexNode, (zone))
#define RETIRE_NODE(retiredList, ptr) retireElement((retiredList), (ptr), reclaimDataLayerNode, 0)

#endif
