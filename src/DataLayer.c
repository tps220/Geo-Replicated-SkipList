#ifndef DATA_LAYER_C
#define DATA_LAYER_C

#include "DataLayer.h"
#include <pthread.h>
#include <assert.h>
#include <unistd.h>

//Helper Threads
dataLayerThread_t* propogator = NULL;
dataLayerThread_t* remover = NULL;
notification_queue_t** notifications = NULL;
extern unsigned int numThreads;

//Helper Functions
inline pair_t getElement(inode_t* sentinel, const int val, HazardNode_t* hazardNode);
inline void dispatchSignal(int val, node_t* dataLayer, Job operation);
inline int validateLink(node_t* previous, node_t* current);
inline int validateRemoval(node_t* previous, node_t* current);
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
    if (validateRemoval(prv, curr)) {
      notify(notifications[hazardNode -> id], curr);
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
  return (volatile node_t*)previous -> next == current &&
         previous -> markedToDelete != PHYSICAL &&
         current -> markedToDelete != PHYSICAL;
}

inline int validateRemoval(node_t* previous, node_t* current) {
  return current -> markedToDelete == LOGICAL &&
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
      insertion -> previous = previous;

      current -> previous = insertion;
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
        if (validateRemoval(previous, current)) {
          notify(notifications[hazardNode -> id], current);
        }
        pthread_mutex_unlock(&previous -> lock);
        pthread_mutex_unlock(&current -> lock);
        return 0;
      }
      current -> markedToDelete = LOGICAL;
      if (validateRemoval(previous, current)) {
        notify(notifications[hazardNode -> id], current);
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

void* backgroundPropogation(void* input) {
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
        else {
          current -> references += numberNumaZones;
          dispatchSignal(current -> val, current, INSERTION);
        }
      }
      hazardNode -> hp0 = current;
      previous = (node_t*)hazardNode -> hp0;
      hazardNode -> hp1 = previous -> next;
      current = (node_t*)hazardNode -> hp1;
    }
 
  int size = 0;
  for (int i = 0; i < numThreads + 1; i++) {
      notification_t* runner = notifications[i] -> head;
      while (runner != NULL) {
          size++;
          runner = runner -> next;
      }
  }
  fprintf(stderr, "SIZE OF %d left\n", size);
  }
  return NULL;
}

int processNotifications(notification_queue_t *threadNotifications, HazardNode_t* hazardNode) {
  notification_t* notification = processNotification(threadNotifications);
  if (notification != NULL) {
    node_t* target = notification -> node;
    node_t* previous = target -> previous;

    pthread_mutex_lock(&previous -> lock);
    pthread_mutex_lock(&target -> lock);
    int valid;
    if ((valid = validateRemoval(previous, target)) != 0) {
      target -> fresh = 0;
      target -> markedToDelete = 2;
      previous -> next = target -> next;
      target -> next -> previous = previous;
    }
    pthread_mutex_unlock(&previous -> lock);
    pthread_mutex_unlock(&target -> lock);
    if (valid) {
      RETIRE_NODE(hazardNode, target);
    }
    else {
      __sync_fetch_and_or(&target -> inQueue, 0);
    }
    free(notification);
    return 1;
  }
  free(notification);
  return 0;
}

void* backgroundNotificationConsumer(void* input) {
  dataLayerThread_t* thread = (dataLayerThread_t*)input;
  while (thread -> finished == 0) {
    for (int i = 0; i < numThreads + 1; i++) {
      processNotifications(notifications[i], thread -> hazardNode);
    }
  }
  return NULL;
}

inline void initializeNotifications() {
  notifications = (notification_queue_t**)malloc((numThreads + 1) * sizeof(notification_queue_t*));
  for (int i = 0; i < numThreads + 1; i++) {
    notifications[i] = constructNotificationQueue();
  }
}

inline void destructNotifications() {
  for (int i = 0; i < numThreads + 1; i++) {
    destructNotificationQueue(notifications[i]);
  }
  free(notifications);
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
  propogator = constructDataLayerThread(hazardNode);
  propogator -> finished = 0;
  propogator -> sentinel = sentinel;
  pthread_create(&propogator -> runner, NULL, backgroundPropogation, (void*)propogator);
  propogator -> running = 1;

  initializeNotifications();
  remover = constructDataLayerThread(hazardNode);
  remover -> finished = 0;
  pthread_create(&remover -> runner, NULL, backgroundNotificationConsumer, (void*)remover);
  propogator -> running = 1;
}

void stopDataLayerHelpers() {
  propogator -> finished = 1;
  pthread_join(propogator -> runner, NULL);
  propogator -> running = 0;
  //destructDataLayerThread(propogator);

  remover -> finished = 1;
  pthread_join(remover -> runner, NULL);
  remover -> running = 0;
  //destructDataLayerThread(remover);
  destructNotifications();

}

#endif
