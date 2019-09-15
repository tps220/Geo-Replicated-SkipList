#ifndef HAZARD_C
#define HAZARD_C
#include "Hazard.h"
#include "Nodes.h"

HazardNode_t* constructHazardNode(unsigned int zone, unsigned int id) {
    HazardNode_t* node = (HazardNode_t*)nalloc(allocators[zone], sizeof(HazardNode_t));
    node -> hp0 = node -> hp1 = NULL;
    node -> id = id;
    node -> next = NULL;
    node -> retiredList = constructLinkedList();
    return node;
}

void destructHazardNode(HazardNode_t* node, unsigned int zone, int isDataLayer) {
  destructLinkedList(node -> retiredList, isDataLayer);
  nfree(allocators[zone], node, sizeof(HazardNode_t));
}

HazardContainer_t* constructHazardContainer(HazardNode_t* head) {
    HazardContainer_t* container = (HazardContainer_t*)malloc(sizeof(HazardContainer_t));
    container -> head = head;
    return container;
}

void destructHazardContainer(HazardContainer_t* container) {
  free(container);
}

void retireElement(LinkedList_t* retiredList, void* ptr, void (*reclaimMemory)(void*, int), unsigned int zone) {
  ll_push(retiredList, ptr);
  if (retiredList -> size >= MAX_DEPTH) {
    scan(retiredList, reclaimMemory, zone);
  }
}

void scan(LinkedList_t* retiredList, void (*reclaimMemory)(void*, int), unsigned int zone) {
  //Collect all valid hazard pointers across application threads
  LinkedList_t* ptrList = constructLinkedList();
  HazardNode_t* runner = memoryLedger -> head;
   __sync_synchronize();
  while (runner != NULL) {
    volatile void* hp0 = (volatile void*)runner -> hp0;
    volatile void* hp1 = (volatile void*)runner -> hp1;
    if (hp0 != NULL) {
      ll_push(ptrList, hp0);
    }
    if (hp1 != NULL) {
      ll_push(ptrList, hp1);
    }
    runner = runner -> next;
  }

  //Compare retired candidates against active hazard nodes, reclaiming or procastinating
  int listSize = retiredList -> size;
  void** tmpList = (void**)malloc(listSize * sizeof(void*));
  ll_pipeAndRemove(retiredList, tmpList);
  for (int i = 0; i < listSize; i++) {
    if (findElement(ptrList, tmpList[i])) {
      ll_push(retiredList, tmpList[i]);
    }
    else {
      reclaimMemory(tmpList[i], zone);
    }
  }
  free(tmpList);
}

void reclaimIndexNode(void* ptr, unsigned int zone) {
  inode_t* node = (inode_t*)ptr;
  destructIndexNode(node, zone);
  node = NULL;
}

void reclaimDataLayerNode(void* ptr, unsigned int zone) {
  free(ptr);
  ptr = NULL;
}

#endif
