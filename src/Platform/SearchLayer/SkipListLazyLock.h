#ifndef SKIPLISTLAZYLOCK_H
#define SKIPLISTLAZYLOCK_H

#include "Nodes.h"
#include "MemoryQueue.h"

//driver functions
int add(inode_t *sentinel, int val, node_t* dataLayer, int zone);
int removeNode(inode_t *sentinel, int val, int zone, memory_queue_t* gc) ;

#endif
