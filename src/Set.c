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

int sl_overhead(node_t* sentinel) {
  int size = -1;
  node_t* runner = sentinel -> next;
  while (runner != NULL) {
    size++;
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

//Get data for small --> big (same scheduling mechanism in Herihly)

//Implement hazard pointers for Herihly

//Measure number of nodes we traverse in the data layer in general

//How many times NUMASK fails in unlinking a node in the helepr thread and goes forward

//Create an easy mechanism to remove reclamation or enable reclamation

//This will enable us to prove
