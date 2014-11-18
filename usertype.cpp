#include "usertype.h"
#include <stdio.h>
#include <atomic>
#include <assert.h>

ListNode* List::removeHead() {
    if (head->next == tail) {
        return NULL;
    } else {
        ListNode *node = head->next;

        head->next = node->next;
        node->next->prev = head;
        node->prev = NULL;
        node->next = NULL;

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

        num++;

        return 0;
    } else {
        return -1;
    }
}