#ifndef MEMORY_QUEUE_H_
#define MEMORY_QUEUE_H_

#include "Nodes.h"
#include <stdlib.h>

typedef struct m_node {
	void* node;
	struct m_node* next;
} m_node_t;

m_node_t* constructMNode(void* node);

typedef struct memory_queue {
	m_node_t* head;
	m_node_t* tail;
	m_node_t* sentinel;
} memory_queue_t;

memory_queue_t* constructMemoryQueue();
void destructMemoryQueue(memory_queue_t* jobs);
void mq_push(memory_queue_t* jobs, void* node);
m_node_t* mq_pop(memory_queue_t* jobs);

#endif
