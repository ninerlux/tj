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

int HashTable::add(record_r *r) {
    int hash_key = hash(r->k);
    int i = hash_key;

    while (true) {
        bool has_slot = false;
        do {
            if (probe_slot(i)) {
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
            record_r *none = NULL;
            bool success = __atomic_compare_exchange(table[i], none, r, false, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST);
            if (success) {
                return i;
            }
        } else {
            return -1;
        }
    }
}

int HashTable::find(join_key_t k, record_r *r) {
    int hash_key = hash(k);
    int i = hash_key;

    do {
        if (table[i]->k == k) {
            r = table[i];
            return i;
        } else {
            i++;
        }
        if (i >= num) {
            i = 0;
        }
    } while (i != hash_key);

    return -1;
}

bool HashTable::probe_slot(int index) {
    if (index >= 0 && index < num && table[index] == NULL) {
        return true;
    } else {
        return false;
    }
}


int HashTable::hash(join_key_t k) {
    //not a good function, to be changed
    return k % 8999 % num;
}
