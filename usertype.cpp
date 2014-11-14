#include "usertype.h"
#include <stdio.h>
#include <string.h>
#include <atomic>

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

ListNode* List::removeHeadSafe() {
    ListNode *node;

    lock();
    node = removeHead();
    unlock();

    return node;
}

int List::addTail(ListNode *node) {
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

int List::addTailSafe(ListNode *node) {
    int res;

    lock();
    res = addTail(node);
    unlock();

    return res;
}

int HashTable::hash(join_key_t k) {
    //not a good function, to be changed
    return k % 8999 % num;
}

int HashTable::add(record_r r) {
    int hash_key = hash(r->k);
    int i = hash_key;

    while (true) {
        bool has_slot = false;
        do {
            if (table[i].k == 0) {
                has_slot = true;
                break;
            } else {
                i++;
            }
            if (i >= num) {
                i = 0;
            }
        } while (i != hash_key);

        if (has_slot) {
            bool success = __sync_bool_compare_and_swap(&table[i].k, 0, r);
			if (success) {
                return i;
            }
        } else {
            return -1;
        }
    }
}

int HashTable::find(join_key_t k, record_r *r, int index, size_t nr_results) {
    int hash_key = hash(k);
    int i = hash_key;

	if (index != -1) {
		i = index;
	}

    do {
        if (table[i].k == k) {
            *r = table[i];
            return i;
        } else {
            i++;
        }
        if (i >= num) {
            i = 0;
        }
    } while (table[i].k != 0 || i != hash_key);
	
    return -1;
}


