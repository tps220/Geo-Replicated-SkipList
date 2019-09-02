#ifndef DATA_LAYER_C
#define DATA_LAYER_C

#include "DataLayer.h"
#include <pthread.h>
#include <assert.h>
#include <unistd.h>

//Helper Threads
dataLayerThread_t* remover = NULL;
gc_container_t* gc = NULL;

//Helper Functions
inline node_t* getElement(inode_t* sentinel, const int val, HazardNode_t* hazardNode);
inline void dispatchSignal(int val, node_t* dataLayer, Job operation);
inline int validateLink(node_t* previous, node_t* current);
inline int validateRemoval(node_t* previous, node_t* current);
inline void collect(memory_queue_t* garbage, LinkedList_t* retiredList);

inline dataLayerThread_t* constructDataLayerThread();

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
        curr -> fresh = 0;
        prv -> next = curr -> next;
        multi_push(mq[numberNumaZones + hazardNode -> id], curr);
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
         current -> fresh == 0 &&
         current -> references == 0;
}

int lazyFind(searchLayer_t* numask, int val, HazardNode_t* hazardNode) {
  node_t* current = getElement(numask -> sentinel, val, hazardNode);
  while (current -> val < val) {
    hazardNode -> hp0 = current -> next;
    current = (node_t*)hazardNode -> hp0;
  }
  int found = current -> val == val && current -> markedToDelete == 0;
  hazardNode -> hp0 = NULL;
  hazardNode -> hp1 = NULL;
  return found;
}

int lazyAdd(searchLayer_t* numask, int val, HazardNode_t* hazardNode) {
  char retry = 1;
  while (retry) {
    node_t* previous = getElement(numask -> sentinel, val, hazardNode);
    hazardNode -> hp1 = previous -> next;
    node_t* current = (node_t*)hazardNode -> hp1;
    while (current -> val < val) {
      hazardNode -> hp0 = current;
      previous = (node_t*)hazardNode -> hp0;
      hazardNode -> hp1 = current -> next;
      current = (node_t*)hazardNode -> hp1;
    }
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
    node_t* previous = getElement(numask -> sentinel, val, hazardNode);
    hazardNode -> hp1 = previous -> next;
    node_t* current = (node_t*)hazardNode -> hp1;
    while (current -> val < val) {
      hazardNode -> hp0 = current;
      previous = (node_t*)hazardNode -> hp0;
      hazardNode -> hp1 = current -> next;
      current = (node_t*)hazardNode -> hp1;
    }
    pthread_mutex_lock(&previous -> lock);
    pthread_mutex_lock(&current -> lock);
    hazardNode -> hp0 = NULL;
    hazardNode -> hp1 = NULL;
    if (validateLink(previous, current)) {
      if (current -> val != val || current -> markedToDelete) {
        pthread_mutex_unlock(&previous -> lock);
        pthread_mutex_unlock(&current -> lock);
        return 0;
      }
      current -> markedToDelete = 1;
      current -> fresh = 1;
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
        int valid = 0;
        pthread_mutex_lock(&previous -> lock);
        pthread_mutex_lock(&current -> lock);
        if ((valid = validateRemoval(previous, current)) != 0) {
          current -> markedToDelete = 2;
          previous -> next = current -> next;
        }
        pthread_mutex_unlock(&previous -> lock);
        pthread_mutex_unlock(&current -> lock);
        if (valid) {
          mq_push(gc -> garbage, current);
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
  return thread;
}

inline void destructDataLayerThread(dataLayerThread_t* thread) {
  free(thread);
  thread = NULL;
}

inline gc_container_t* construct_gc_container() {
  gc_container_t* container = (gc_container_t*)malloc(sizeof(gc_container_t));
  container -> garbage = constructMemoryQueue();
  container -> retiredList = constructLinkedList();
  container -> stopGarbageCollection = 0;
  return container;
}

inline void destruct_gc_container(gc_container_t* gc) {
  destructMemoryQueue(gc -> garbage);
  destructLinkedList(gc -> retiredList, 1);
  free(gc);
}

void startDataLayerHelpers(node_t* sentinel) {
  if (remover == NULL) {
    remover = constructDataLayerThread();
  }
  if (gc == NULL) {
    gc = construct_gc_container();
  }
  if (remover -> running == 0) {
    gc -> stopGarbageCollection = 0;
    pthread_create(&gc -> reclaimer, NULL, garbageCollectDataLayer, (void*)gc);

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

    gc -> stopGarbageCollection = 1;
    pthread_join(gc -> reclaimer, NULL);
    destruct_gc_container(gc);
  }
}

void* garbageCollectDataLayer(void* args) {
  gc_container_t* gc = (gc_container_t*)args;

  while (gc -> stopGarbageCollection == 0) {
    usleep(1000);
    collect(gc -> garbage, gc -> retiredList);
  }
  collect(gc -> garbage, gc -> retiredList);
}

inline void collect(memory_queue_t* garbage, LinkedList_t* retiredList) {
  m_node_t* subscriber;
  while ((subscriber = mq_pop(garbage)) != NULL) {
    RETIRE_NODE(retiredList, subscriber -> node);
    free(subscriber);
  }
}
#endif
