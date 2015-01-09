#ifndef _HASHTABLE_H_
#define _HASHTABLE_H_

#include <stdio.h>

using namespace std;

template<typename Record>
class HashTable {
public:
    HashTable(size_t s) : size(s) {
        table = new Record *[size];
        hash32_factor = 79;
    };

    size_t hash32(join_key_t k);

    size_t add(Record *r);
    size_t del(size_t index);
    size_t update(Record *r, size_t index);
    size_t find(join_key_t k, Record **r, size_t index, size_t nr_results);    //index: starting searching index
    size_t markVisited(join_key_t k, size_t index, char table_type, Record **r, bool visited);

    size_t getNextKey(size_t index, join_key_t &k, bool visited);

    size_t getSize() {
        return size;
    }

	void printAll(int local_host);
	Record **table;

private:
    size_t hash32_factor;
    size_t size;
    //Record **table;
};

template<typename Record>
size_t HashTable<Record>::hash32(join_key_t k) {
    uint64_t hash = (uint32_t)(k * hash32_factor);
    size_t res = (hash * size) >> 32;
    return res;
}

template<typename Record>
void HashTable<Record>::printAll(int local_host) {
	//printf("HashTable - printAll\n");
	//fflush(stdout);
	for (size_t i = 0; i < size; i++) {
		if (table[i] != NULL) {
			printf("HashTable - printAll: Node %d, index %lu: key %u, src %u, type %c\n", local_host, i, table[i]->k, table[i]->src, table[i]->table_type);
			fflush(stdout);
		} 
	}
}

template<typename Record>
size_t HashTable<Record>::add(Record *r) {
    size_t hash_key = hash32(r->k);
    size_t i = hash_key;

    while (true) {
		//printf("inside add! key = %u\n", r->k);
        bool has_slot = false;
        do {
            if (table[i] == NULL) {
                has_slot = true;
                break;
            } else {
                i++;
            }
            if (i == size) {
                i = 0;
            }
        } while (i != hash_key);

        if (has_slot) {
            bool success = __sync_bool_compare_and_swap(&table[i], NULL, r);
            if (success) {
                return i;
            }
        } else {
            return size;
        }
    }
}

template<typename Record>
size_t HashTable<Record>::del(size_t index) {
    if (index > 0 && index < size && table[index] != NULL) {
        delete table[index];
        table[index] = NULL;
        return index;
    }

    return size;
}

template<typename Record>
size_t HashTable<Record>::update(Record *r, size_t index) {
    if (index > 0 && index < size) {
        // notice that *r will be revoked later as it is a local variable
        // so we need to copy value here instead of address
        *(table[index]) = *r;
        return index;
    }

    return size;
}


template<typename Record>
size_t HashTable<Record>::find(join_key_t k, Record **r, size_t index, size_t nr_results) {
    size_t hash_key = hash32(k);
    size_t i = hash_key;

    if (index < size) {
        i = index;
    } else if (index == size) {
        i = 0;
    }

    do {
        if (table[i] != NULL) {
            if (table[i]->k == k) {
                *r = table[i];
                return i;
            } else {
                i++;
                if (i == size) {
                    i = 0;
                }
            }
        }
    } while (table[i] != NULL && i != hash_key);

    return size;
}

template<typename Record>
size_t HashTable<Record>::getNextKey(size_t index, join_key_t &k, bool visited) {
	for (size_t i = index; i < size; i++) {
		if (table[i] != NULL && table[i]->visited != visited) {
			k = table[i]->k;
			return i;
		}
	}

	return size;
}

// used only for record_key type
template<typename Record>
size_t HashTable<Record>::markVisited(join_key_t k, size_t index, char table_type, Record **r, bool visited) {
    size_t i = index;
    if (index == size) {
        i = 0;
    }

    do {
        if (table[i] != NULL) {
            if (table[i]->k == k && (table_type == 'N' || table[i]->table_type == table_type)) {
                *r = table[i];
                // mark it as visited
                table[i]->visited = visited;
                return i;
            } else {
                i++;
                if (i == size) {
                    i = 0;
                }
            }
        }
    } while (table[i] != NULL && i != index);

    return size;
}

#endif
