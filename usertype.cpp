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

size_t HashTable::hash32(join_key_t k) {
	uint64_t hash = (uint32_t) (k * hash32_factor);
	size_t res = (hash * num) >> 32;
	//assert(res >= 0 && res < num);
	return res; 
}

int HashTable::add(record_r *r) {
    size_t hash_key = hash32(r->k);
    size_t i = hash_key;

    while (true) {
		bool has_slot = false;
        do {
            if (table[i] == NULL) {
                has_slot = true;
				break;
            } else {
                i++;
            }
            if (i == num) {
                i = 0;
            }
        } while (i != hash_key);

        if (has_slot) {
            bool success = __sync_bool_compare_and_swap(&table[i], NULL, r);
			if (success) {
                return i;
            }
        } else {
            return -1;
        }
    }
}

int HashTable::find(join_key_t k, record_r **r, size_t index, size_t nr_results) {
    size_t hash_key = hash32(k);
    size_t i = hash_key;

	if (index < num) {
		i = index;
	}

    do {
        if (table[i] != NULL && table[i]->k == k) {
            *r = table[i];
            return i;
        } else {
            i++;
        }
        if (i == num) {
            i = 0;
        }
    } while (table[i] != NULL && i != hash_key);
	
    return -1;
}
