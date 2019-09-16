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

inline int removeElement(node_t* previous, node_t* current, HazardNode_t* hazardNode) {
  current -> markedToDelete = 2;
  current -> fresh = 0;
  int retval;
  if (retval = (__sync_fetch_and_add(&current -> references, 0) != 0)) {
      current -> markedToDelete = LOGICAL;
      current -> fresh = 1;
      return 0;
  }
  else {
    previous -> next = (volatile node_t*)current -> next;
    return 1;
  }
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
    int valid;
    if (valid = (validateRemoval(prv, curr) && curr -> next -> val < val)) {
      pthread_mutex_lock(&prv -> lock);
      pthread_mutex_lock(&curr -> lock);
      if (valid = validateRemoval(prv, curr) && curr -> next -> val < val) {
        valid = removeElement(prv, curr, hazardNode);
      }
      pthread_mutex_unlock(&prv -> lock);
      pthread_mutex_unlock(&curr -> lock);
      if (valid) {
        RETIRE_NODE(hazardNode, curr);
        hazardNode -> hp1 = prv -> next;
        curr = hazardNode -> hp1;
        continue;
      }
    }
    hazardNode -> hp0 = curr;
    prv = hazardNode -> hp0;
    hazardNode -> hp1 = prv -> next;
    curr = hazardNode -> hp1;
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
  return (volatile node_t*)previous -> next == current &&
         previous -> markedToDelete != PHYSICAL &&
         current -> markedToDelete != PHYSICAL;
}

inline int validateRemoval(node_t* previous, node_t* current) {
  return (volatile int) current -> markedToDelete == LOGICAL &&
         validateLink(previous, current) &&
         current -> references == 0;
}

int lazyFind(searchLayer_t* numask, int val, HazardNode_t* hazardNode) {
  pair_t pair = getElement(numask -> sentinel, val, hazardNode);
  node_t* current = pair.current;
  return current -> val == val && current -> markedToDelete == EMPTY;
}

int lazyAdd(searchLayer_t* numask, int val, HazardNode_t* hazardNode) {
  char retry = 1;
  while (retry) {
    pair_t pair = getElement(numask -> sentinel, val, hazardNode);
    node_t* previous = pair.previous;
    node_t* current = pair.current;
    pthread_mutex_lock(&previous -> lock);
    pthread_mutex_lock(&current -> lock);
    if (validateLink(previous, current)) {
      if (current -> val == val && current -> markedToDelete) {
        current -> markedToDelete = EMPTY;
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
    if (validateLink(previous, current)) {
      if (current -> val != val || current -> markedToDelete) {
        int valid;
        if (valid = (validateRemoval(previous, current))) {
          valid = removeElement(previous, current, hazardNode);
        }
        pthread_mutex_unlock(&previous -> lock);
        pthread_mutex_unlock(&current -> lock);
        if (valid) {
          RETIRE_NODE(hazardNode, current); 
        }
        return 0;
      }
      current -> markedToDelete = LOGICAL;
      int valid;
      if (valid = (validateRemoval(previous, current))) {
        valid = removeElement(previous, current, hazardNode);
      }
      else {
        current -> fresh = 1;
      }
      pthread_mutex_unlock(&previous -> lock);
      pthread_mutex_unlock(&current -> lock);
      if (valid) {
        RETIRE_NODE(hazardNode, current);
      }
      return 1;
    }
    pthread_mutex_unlock(&previous -> lock);
    pthread_mutex_unlock(&current -> lock);
  }
}

void* backgroundRemoval(void* input) {
  dataLayerThread_t* thread = (dataLayerThread_t*)input;
  HazardNode_t* hazardNode = thread -> hazardNode;
  node_t* sentinel = thread -> sentinel;
  while (thread -> finished == 0) {
    usleep(thread -> sleep_time);
    node_t* previous = sentinel;
    hazardNode -> hp1 = sentinel -> next;
    node_t* current = (node_t*)hazardNode -> hp1;
    while (current -> next != NULL) {
      if (current -> fresh) {
        current -> fresh = 0;
        if (current -> markedToDelete == LOGICAL) {
          dispatchSignal(current -> val, current, REMOVAL);
        }
        else if (current -> markedToDelete == EMPTY) {
          //__sync_synchronize();
          __sync_fetch_and_add(&current -> references, numberNumaZones);
          if (__sync_fetch_and_add(&current -> markedToDelete, 0) != EMPTY) {
            current -> references -= numberNumaZones;
          }
          else {
            dispatchSignal(current -> val, current, INSERTION);
          }
        }
      }/*
      else if (validateRemoval(previous, current)) {
        pthread_mutex_lock(&previous -> lock);
        pthread_mutex_lock(&current -> lock);
        int valid = 0;
        if ((valid = validateRemoval(previous, current)) != 0) {
          //removeElement(previous, current, thread -> hazardNode);
        }
        pthread_mutex_unlock(&previous -> lock);
        pthread_mutex_unlock(&current -> lock);
        if (valid) {
          hazardNode -> hp1 = previous -> next;
          current = (node_t*)hazardNode -> hp1;
          continue;
        }
      }
      */
      hazardNode -> hp0 = current;
      previous = (node_t*)hazardNode -> hp0;
      hazardNode -> hp1 = previous -> next;
      current = (node_t*)hazardNode -> hp1;
    }
  }
  return NULL;
}

inline dataLayerThread_t* constructDataLayerThread(HazardNode_t* hazardNode) {
  dataLayerThread_t* thread = (dataLayerThread_t*)malloc(sizeof(dataLayerThread_t));
  thread -> running = 0;
  thread -> finished = 0;
  thread -> sleep_time = 10000;
  thread -> sentinel = NULL;
  thread -> hazardNode = hazardNode;
  return thread;
}

inline void destructDataLayerThread(dataLayerThread_t* thread) {
  //destructHazardNode(thread -> hazardNode);
  free(thread);
  thread = NULL;
}

void startDataLayerHelpers(node_t* sentinel, HazardNode_t* hazardNode) {
  if (remover == NULL) {
    remover = constructDataLayerThread(hazardNode);
  }
  if (remover -> running == 0) {
    remover -> finished = 0;
    remover -> sentinel = sentinel;
    pthread_create(&remover -> runner, NULL, backgroundRemoval, (void*)remover);
    remover -> running = 1;
  }
}

void stopDataLayerHelpers(HazardNode_t* hazardNode) {
  if (remover -> running) {
    remover -> finished = 1;
    pthread_join(remover -> runner, NULL);
    remover -> running = 0;
    destructDataLayerThread(remover);
  }
}

#endif
