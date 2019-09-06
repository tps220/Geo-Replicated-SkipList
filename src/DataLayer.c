#ifndef DATA_LAYER_C
#define DATA_LAYER_C

#include "DataLayer.h"
#include <pthread.h>
#include <assert.h>
#include <unistd.h>

//Helper Threads
dataLayerThread_t* remover = NULL;

//Helper Functions
inline pair_t getElement(inode_t* sentinel, const int val, HazardNode_t* hazardNode);
inline void dispatchSignal(int val, node_t* dataLayer, Job operation);
inline int validateLink(node_t* previous, node_t* current);
inline int validateRemoval(node_t* previous, node_t* current);
inline dataLayerThread_t* constructDataLayerThread();

inline void removeElement(node_t* previous, node_t* current, HazardNode_t* hazardNode) {
  current -> markedToDelete = 2;
  current -> fresh = 0;
  previous -> next = current -> next;
  RETIRE_NODE(hazardNode, current);
}

inline pair_t getElement(inode_t* sentinel, const int val, HazardNode_t* hazardNode) {
  inode_t *previous = sentinel, *current = NULL;
  for (int i = previous -> topLevel - 1; i >= 0; i--) {
    hazardNode -> hp1 = previous -> next[i];
    current = (inode_t*)hazardNode -> hp1;
    while (current -> val < val) {
      hazardNode -> hp0 = current;
      previous = (inode_t*)hazardNode -> hp0 ;
      hazardNode -> hp1 = current -> next[i];
      current = (inode_t*)hazardNode -> hp1;
    }
  }

  hazardNode -> hp0 = previous -> dataLayer;
  node_t* prv =  (node_t*)hazardNode -> hp0;
  hazardNode -> hp1 = prv -> next;
  node_t* curr = (node_t*)hazardNode -> hp1;
  while (curr -> val < val) {
    if (validateRemoval(prv, curr) && curr -> next -> val < val) {
      pthread_mutex_lock(&prv -> lock);
      pthread_mutex_lock(&curr -> lock);
      if (validateRemoval(prv, curr) && curr -> next -> val < val) {
        removeElement(prv, curr, hazardNode);
      }
      pthread_mutex_unlock(&prv -> lock);
      pthread_mutex_unlock(&curr -> lock);
    }
    hazardNode -> hp0 = curr;
    prv = (node_t*)hazardNode -> hp0;
    hazardNode -> hp1 = curr -> next;
    curr = (node_t*)hazardNode -> hp1;
  }

  pair_t pair;
  pair.previous = prv;
  pair.current = curr;
  return pair;
}

inline void dispatchSignal(int val, node_t* dataLayer, Job operation) {
  assert(numaLayers != NULL);
  for (int i = 0; i < numberNumaZones; i++) {
    push(numaLayers[i] -> updates, val, operation, dataLayer);
  }
}

inline int validateLink(node_t* previous, node_t* current) {
  return previous -> next == current;
}

inline int validateRemoval(node_t* previous, node_t* current) {
  return previous -> next == current &&
         current -> markedToDelete &&
         current -> references == 0;
}

int lazyFind(searchLayer_t* numask, int val, HazardNode_t* hazardNode) {
  pair_t pair = getElement(numask -> sentinel, val, hazardNode);
  node_t* current = pair.current;
  int found = current -> val == val && current -> markedToDelete == 0;
  hazardNode -> hp0 = NULL;
  hazardNode -> hp1 = NULL;
  return found;
}

int lazyAdd(searchLayer_t* numask, int val, HazardNode_t* hazardNode) {
  char retry = 1;
  while (retry) {
    pair_t pair = getElement(numask -> sentinel, val, hazardNode);
    node_t* previous = pair.previous;
    node_t* current = pair.current;
    pthread_mutex_lock(&previous -> lock);
    pthread_mutex_lock(&current -> lock);
    hazardNode -> hp0 = NULL;
    hazardNode -> hp1 = NULL;
    if (validateLink(previous, current) && previous -> markedToDelete != 2) {
      if (current -> val == val && current -> markedToDelete) {
        current -> markedToDelete = 0;
        current -> fresh = 2;
        pthread_mutex_unlock(&previous -> lock);
        pthread_mutex_unlock(&current -> lock);
        return 1;
      }
      else if (current -> val == val) {
        pthread_mutex_unlock(&previous -> lock);
        pthread_mutex_unlock(&current -> lock);
        return 0;
      }
      node_t* insertion = constructNode(val, numberNumaZones); //automatically set as fresh
      insertion -> next = current;
      previous -> next = insertion;
      pthread_mutex_unlock(&previous -> lock);
      pthread_mutex_unlock(&current -> lock);
      return 1;
    }
    pthread_mutex_unlock(&previous -> lock);
    pthread_mutex_unlock(&current -> lock);
  }
}

int lazyRemove(searchLayer_t* numask, int val, HazardNode_t* hazardNode) {
  char retry = 1;
  while (retry) {
    pair_t pair = getElement(numask -> sentinel, val, hazardNode);
    node_t* previous = pair.previous;
    node_t* current = pair.current;
    pthread_mutex_lock(&previous -> lock);
    pthread_mutex_lock(&current -> lock);
    hazardNode -> hp0 = NULL;
    hazardNode -> hp1 = NULL;
    if (validateLink(previous, current)) {
      if (current -> val != val || current -> markedToDelete) {
        if (validateRemoval(previous, current)) {
          removeElement(previous, current, hazardNode);
        }
        pthread_mutex_unlock(&previous -> lock);
        pthread_mutex_unlock(&current -> lock);
        return 0;
      }
      current -> markedToDelete = 1;
      if (validateRemoval(previous, current)) {
        removeElement(previous, current, hazardNode);
      }
      else {
        current -> fresh = 1;
      }
      pthread_mutex_unlock(&previous -> lock);
      pthread_mutex_unlock(&current -> lock);
      return 1;
    }
    pthread_mutex_unlock(&previous -> lock);
    pthread_mutex_unlock(&current -> lock);
  }
}

void* backgroundRemoval(void* input) {
  dataLayerThread_t* thread = (dataLayerThread_t*)input;
  node_t* sentinel = thread -> sentinel;
  while (thread -> finished == 0) {
    usleep(thread -> sleep_time);
    node_t* previous = sentinel;
    node_t* current = sentinel -> next;
    while (current -> next != NULL) {
      if (current -> fresh) {
        current -> fresh = 0;
        if (current -> markedToDelete) {
          dispatchSignal(current -> val, current, REMOVAL);
        }
        else {
          current -> references += numberNumaZones;
          dispatchSignal(current -> val, current, INSERTION);
        }
      }
      else if (validateRemoval(previous, current)) {
        pthread_mutex_lock(&previous -> lock);
        pthread_mutex_lock(&current -> lock);
        int valid = 0;
        if ((valid = validateRemoval(previous, current)) != 0) {
          removeElement(previous, current, thread -> hazardNode);
        }
        pthread_mutex_unlock(&previous -> lock);
        pthread_mutex_unlock(&current -> lock);
        if (valid) {
          current = previous -> next;
          continue;
        }
      }
      previous = current;
      current = current -> next;
    }
  }
  return NULL;
}

inline dataLayerThread_t* constructDataLayerThread() {
  dataLayerThread_t* thread = (dataLayerThread_t*)malloc(sizeof(dataLayerThread_t));
  thread -> running = 0;
  thread -> finished = 0;
  thread -> sleep_time = 10000;
  thread -> sentinel = NULL;
  thread -> hazardNode = constructHazardNode(0);
  return thread;
}

inline void destructDataLayerThread(dataLayerThread_t* thread) {
  //destructHazardNode(thread -> hazardNode);
  free(thread);
  thread = NULL;
}

void startDataLayerHelpers(node_t* sentinel) {
  if (remover == NULL) {
    remover = constructDataLayerThread();
  }
  if (remover -> running == 0) {
    remover -> finished = 0;
    remover -> sentinel = sentinel;
    pthread_create(&remover -> runner, NULL, backgroundRemoval, (void*)remover);
    remover -> running = 1;
  }
}

void stopDataLayerHelpers() {
  if (remover -> running) {
    remover -> finished = 1;
    pthread_join(remover -> runner, NULL);
    remover -> running = 0;
    destructDataLayerThread(remover);
  }
}

#endif
