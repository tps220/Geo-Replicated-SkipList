#ifndef LINKED_LIST_C
#define LINKED_LIST_C

#include "LinkedList.h"

ListNode_t* constructListNode(void* ptr) {
  ListNode_t* node = (ListNode_t*)malloc(sizeof(ListNode_t));
  node -> data = ptr;
  node -> next = NULL;
  return node;
}

void destructListNode(ListNode_t* node, int isDataLayer) {
  if (isDataLayer) {
    free(node -> data);
  }
  free(node);
}

LinkedList_t* constructLinkedList() {
  LinkedList_t* ll = (LinkedList_t*)malloc(sizeof(LinkedList_t));
  ll -> head = ll -> tail = NULL;
  ll -> size = 0;
  return ll;
}

void destructLinkedList(LinkedList_t* ll, int isDataLayer) {
  ListNode_t* runner = ll -> head;
  while (runner != NULL) {
    ListNode_t* temp = runner;
    runner = runner -> next;
    destructListNode(temp, isDataLayer);
  }
  free(ll);
}

int findElement(LinkedList_t* ll, void* node) {
  ListNode_t* runner = ll -> head;
  while (runner != NULL) {
    if (runner -> data == node) {
      return 1;
    }
    runner = runner -> next;
  }
  return 0;
}

void ll_push(LinkedList_t* ll, void* ptr) {
  if (ll -> head == NULL) {
    ll -> head = ll -> tail = constructListNode(ptr);
  }
  else {
    ll -> tail -> next = constructListNode(ptr);
    ll -> tail = ll -> tail -> next;
  }
  ll -> size++;
}

void* ll_pop(LinkedList_t* ll) {
  if (ll -> head == NULL) {
    return NULL;
  }
  void* data = ll -> head -> data;
  if (ll -> head == ll -> tail) {
    free(ll -> head);
    ll -> head = ll -> tail = NULL;
  }
  else {
    ListNode_t* temp = ll -> head;
    ll -> head = ll -> head -> next;
    free(temp);
  }
  ll -> size--;
  return data;
}

int ll_pipeAndRemove(LinkedList_t* ll, void** output) {
  ListNode_t* runner = ll -> head;
  int count = 0;
  while (runner != NULL) {
    output[count] = runner -> data;
    count++;

    ListNode_t* temp = runner;
    runner = runner -> next;
    free(temp);
  }
  ll -> head = ll -> tail = NULL;
  ll -> size = 0;
  return count;
}

#endif
