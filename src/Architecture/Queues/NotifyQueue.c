#ifndef JOB_QUEUE_C
#define JOB_QUEUE_C

#include "JobQueue.h"

notification_t* constructNotification(node_t* node) {
  notification_t* notification = (notification_t*)malloc(sizeof(notification_t));
  notification -> node = node;
  notification -> next = NULL;
  return notification;
}

notification_t* constructNotificationQueue() {
  notification_queue_t* q = (notification_queue_t*)malloc(sizeof(notification_queue_t));
  q -> sentinel = constructNotification(NULL);
  q -> tail = q -> head = q -> sentinel;
  return q;
}

void destructNotificationQueue(notification_queue_t* q) {
  notification_t* runner = q -> head;
  while (runner != NULL) {
    notification_t* temp = runner;
    runner = runner -> next;
    free(temp);
  }
  free(q);
}

void notify(notification_queue_t* q, node_t* node) {
  if (__sync_fetch_and_or(&node -> inQueue, 1)) {
    return;
  }
  notification_t* notification = constructNotification(node);
  q -> tail -> next = notification;
  q -> tail = notification;
}

notification_t* processNotification(notification_queue_t* q) {
  if (q -> head -> next == NULL) {
    return NULL;
  }
  notification_t* front = q -> head;
  q -> head = q -> head -> next;
  front -> node = q -> head -> node;
  return front;
}

#endif
