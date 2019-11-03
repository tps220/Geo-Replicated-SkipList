#ifndef DATA_LAYER_C
#define DATA_LAYER_C

#define TX_START gc_crit_enter(gc);
#define TX_END gc_crit_exit(gc);
#define RECLAIM(curr) gc_limbo(gc, (curr))

#include "DataLayer.h"
#include <pthread.h>
#include <assert.h>
#include <unistd.h>
//Helper Threads
dataLayerThread_t* remover = NULL;

//Helper Functions
inline pair_t getElement(inode_t* sentinel, const int val);
inline void dispatchSignal(int val, node_t* dataLayer, Job operation);
inline int validateLink(node_t* previous, node_t* current);
inline int validateRemoval(node_t* previous, node_t* current);
inline dataLayerThread_t* constructDataLayerThread();

inline int removeElement(node_t* previous, node_t* current) {
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

inline pair_t getElement(inode_t* sentinel, const int val) {
  inode_t *previous = sentinel, *current = NULL;
  for (int i = previous -> topLevel - 1; i >= 0; i--) {
    current = previous -> next[i];
    while (current -> val < val) {
      previous = current;
      current = current -> next[i];;
    }
  }

  node_t* prv = previous -> dataLayer;
  node_t* curr = prv -> next;
  while (curr -> val < val) {
    int valid;
    if (valid = (validateRemoval(prv, curr) && curr -> next -> val < val)) {
      pthread_mutex_lock(&prv -> lock);
      pthread_mutex_lock(&curr -> lock);
      if (valid = validateRemoval(prv, curr) && curr -> next -> val < val) {
        valid = removeElement(prv, curr);
      }
      pthread_mutex_unlock(&prv -> lock);
      pthread_mutex_unlock(&curr -> lock);
      if (valid) {
        RECLAIM(curr);
        curr = prv -> next;
        continue;
      }
    }
    prv = curr;
    curr = prv -> next;
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

int lazyFind(searchLayer_t* numask, int val) {
  TX_START
  pair_t pair = getElement(numask -> sentinel, val);
  node_t* current = pair.current;
  int retval = current -> val == val && current -> markedToDelete == EMPTY;
  TX_END
  return retval;
}

int lazyAdd(searchLayer_t* numask, int val) {
  TX_START
  char retry = 1;
  while (retry) {
    pair_t pair = getElement(numask -> sentinel, val);
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
        TX_END
        return 1;
      }
      else if (current -> val == val) {
        pthread_mutex_unlock(&previous -> lock);
        pthread_mutex_unlock(&current -> lock);
        TX_END
        return 0;
      }
      node_t* insertion = constructNode(val, numberNumaZones); //automatically set as fresh
      insertion -> next = current;
      previous -> next = insertion;
      pthread_mutex_unlock(&previous -> lock);
      pthread_mutex_unlock(&current -> lock);
      TX_END
      return 1;
    }
    pthread_mutex_unlock(&previous -> lock);
    pthread_mutex_unlock(&current -> lock);
  }
}

int lazyRemove(searchLayer_t* numask, int val) {
  TX_START
  char retry = 1;
  while (retry) {
    pair_t pair = getElement(numask -> sentinel, val);
    node_t* previous = pair.previous;
    node_t* current = pair.current;
    pthread_mutex_lock(&previous -> lock);
    pthread_mutex_lock(&current -> lock);
    if (validateLink(previous, current)) {
      if (current -> val != val || current -> markedToDelete) {
        int valid;
        if (valid = (validateRemoval(previous, current))) {
          valid = removeElement(previous, current);
        }
        pthread_mutex_unlock(&previous -> lock);
        pthread_mutex_unlock(&current -> lock);
        if (valid) {
          RECLAIM(current);
        }
        TX_END
        return 0;
      }
      current -> markedToDelete = LOGICAL;
      int valid;
      if (valid = (validateRemoval(previous, current))) {
        valid = removeElement(previous, current);
      }
      else {
        current -> fresh = 1;
      }
      pthread_mutex_unlock(&previous -> lock);
      pthread_mutex_unlock(&current -> lock);
      if (valid) {
        RECLAIM(current);
      }
      TX_END
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
    node_t* current =  sentinel -> next;
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
      previous = current;
      current = previous -> next;
    }
    gc_cycle(gc);
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
