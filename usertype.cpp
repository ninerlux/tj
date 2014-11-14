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

r_payload_t table_r::key_to_payload(join_key_t k) {
	float a = 181;
	r_payload_t p;
	uint32_t res = (uint32_t)(a * k);
	memcpy(&p, &res, sizeof(r_payload_t)); 
	return p; 
}

join_key_t table_r::payload_to_key(r_payload_t p) {
	float b = 1 / 181;
	uint32_t payload;
	memcpy(&payload, &p, sizeof(r_payload_t));
	join_key_t k = (join_key_t)(b * payload);
	return k;
}

s_payload_t table_s::key_to_payload(join_key_t k) {
	float a = 131;
	s_payload_t p;
	uint32_t res = (uint32_t)(a * k);
	memcpy(&p, &res, sizeof(s_payload_t)); 
	return p; 
}

join_key_t table_s::payload_to_key(s_payload_t p) {
	float b = 1 / 131;
	uint32_t payload;
	memcpy(&payload, &p, sizeof(s_payload_t));
	join_key_t k = (join_key_t)(b * payload);
	return k;
}


int HashTable::hash(join_key_t k) {
    //not a good function, to be changed
    return k % 8999 % num;
}

int HashTable::add(record_r *r) {
    int hash_key = hash(r->k);
    int i = hash_key;

    while (true) {
        bool has_slot = false;
        do {
            if (table[i] == NULL) {
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
            bool success = __sync_bool_compare_and_swap(&table[i], NULL, r);
			if (success) {
                return i;
            }
        } else {
            return -1;
        }
    }
}

int HashTable::find(join_key_t k, int index, record_r **r) {
    int hash_key = hash(k);
    int i = hash_key;

	if (index != -1) {
		i = index;
	}

    do {
        if (table[i] != NULL && table[i]->k == k) {
            *r = table[i];
            return i;
        } else {
            i++;
        }
        if (i >= num) {
            i = 0;
        }
    } while (table[i] != NULL && i != hash_key);
	
    return -1;
}


