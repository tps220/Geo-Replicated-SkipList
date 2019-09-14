#ifndef NOTIFY_QUEUE_H_
#define JOB_QUEUE_H_

#include "Nodes.h"
#include <stdlib.h>

typedef struct notification {
  node_t* node;
  struct q_node* next;
} notification_t;

notification_t* constructNotification(node_t* node);

typedef struct notification_queue {
  notification_t* head;
  notification_t* tail;
  notification_t* sentinel;
} notification_queue_t;

notification_queue_t* constructNotificationQueue();
void destructNotificationQueue(notification_queue_t* jobs);
void notify(notification_queue_t* jobs, node_t* node);
notification_queue_t* processNotification(notification_queue_t* jobs);


#endif
