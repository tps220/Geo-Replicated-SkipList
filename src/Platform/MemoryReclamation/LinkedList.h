#ifndef LINKED_LIST_H
#define LINKED_LIST_H

#include "stdlib.h"

typedef struct ListNode {
  void* data;
  struct ListNode* next;
} ListNode_t;

ListNode_t* constructListNode(void* ptr);
void destructListNode(ListNode_t* node, int isDataLayer);

typedef struct LinkedList {
  ListNode_t* head;
  ListNode_t* tail;
  int size;
} LinkedList_t;

LinkedList_t* constructLinkedList();
void destructLinkedList(LinkedList_t* ll, int isIndexLayer);
int findElement(LinkedList_t* ll, void* node);
void ll_push(LinkedList_t* ll, void* ptr);
void* ll_pop(LinkedList_t* ll);
int ll_pipeAndRemove(LinkedList_t* ll, void** output);

#endif
