#ifndef SET_C
#define SET_C

#include "Set.h"

int sl_contains(searchLayer_t* numask, int val, HazardNode_t* hazardNode) {
  return lazyFind(numask, val, hazardNode);
}

int sl_add(searchLayer_t* numask, int val, HazardNode_t* hazardNode) {
  return lazyAdd(numask, val, hazardNode);
}

int sl_remove(searchLayer_t* numask, int val, HazardNode_t* hazardNode) {
  return lazyRemove(numask, val, hazardNode);
}

int sl_size(node_t* sentinel) {
  int size = -1;
  node_t* runner = sentinel -> next;
  while (runner != NULL) {
    if (!runner -> markedToDelete) {
      size++;
    }
    runner = runner -> next;
  }
  return size;
}

void sl_destruct(node_t* sentinel) {
	node_t* runner = sentinel;
	while (runner != NULL) {
		node_t* temp = runner;
		runner = runner -> next;
		free(temp);
	}
}

#endif
