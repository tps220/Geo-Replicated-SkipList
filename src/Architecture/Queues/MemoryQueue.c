#ifndef MEMORY_QUEUE_C
#define MEMORY_QUEUE_C

#include "MemoryQueue.h"

m_node_t* constructMNode(void* node) {
	m_node_t* new_job = (m_node_t*)malloc(sizeof(m_node_t));
	new_job -> node = node;
  new_job -> next = NULL;
	return new_job;
}

memory_queue_t* constructMemoryQueue() {
	memory_queue_t* jobs = (memory_queue_t*)malloc(sizeof(memory_queue_t));
	jobs -> sentinel = constructMNode(NULL);
	jobs -> tail = jobs -> head = jobs -> sentinel;
	return jobs;
}

void destructMemoryQueue(memory_queue_t* jobs) {
	m_node_t* runner = jobs -> head;
	while (runner != NULL) {
		m_node_t* temp = runner;
		runner = runner -> next;
		free(temp);
	}
	free(jobs);
}

void mq_push(memory_queue_t* jobs, void* node) {
	m_node_t* new_job = constructMNode(node);
	jobs -> tail -> next = new_job;
	jobs -> tail = new_job;
}

m_node_t* mq_pop(memory_queue_t* jobs) {
	if (jobs -> head -> next == NULL) {
		return NULL;
	}
	m_node_t* front = jobs -> head;
	jobs -> head = jobs -> head -> next;
  front -> node = jobs -> head -> node;
	return front;
}

#endif
