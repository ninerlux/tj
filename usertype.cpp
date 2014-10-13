#include "usertype.h"

ListNode* List::removeHead() {
    if (head->next == tail) {
        return NULL;
    } else {
        ListNode *node = head->next;
        head->next = node->next;
        node->next->prev = head;
        node->prev = NULL;
        node->next = NULL;
        //update num of nodes
        num--;
        return node;
    }
}

int List::addTail(ListNode * node) {
    if (node != NULL) {
        node->next = tail;
        node->prev = tail->prev;
        tail->prev->next = node;
        tail->prev = node;
        //update num of nodes
        num++;
        return 0;
    } else {
        return -1;
    }
}