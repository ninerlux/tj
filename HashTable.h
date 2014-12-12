#ifndef _HASHTABLE_H_
#define _HASHTABLE_H_

template<typename Record>
class HashTable {
public:
    //local HashTable for hash join
    //The HashTable stores keys and payloads in table R
    HashTable(size_t s) : size(s) {
        table = new Record *[size];
        hash32_factor = 79;
    };

    size_t hash32(join_key_t k);

    size_t add(Record *r);

    size_t find(join_key_t k, Record **r, size_t index, size_t nr_results);    //index: starting searching index
    size_t markUsed(join_key_t k, size_t index, char table_type, int &node_nr, size_t nr_results);

    size_t getNextKey(size_t index, join_key_t &k);

    size_t getSize() {
        return size;
    }

private:
    size_t hash32_factor;
    size_t size;
    Record **table;
};

template<typename Record>
size_t HashTable<Record>::hash32(join_key_t k) {
    uint64_t hash = (uint32_t)(k * hash32_factor);
    size_t res = (hash * size) >> 32;
    return res;
}

template<typename Record>
size_t HashTable<Record>::add(Record *r) {
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
size_t HashTable<Record>::getNextKey(size_t index, join_key_t &k) {
    for (size_t i = index; i < size; i++) {
        if (table[i] != NULL) {
            k = table[i]->k;
            return i;
        }
    }

    return size;
}

// used only for record_key type
template<typename Record>
size_t HashTable<Record>::markUsed(join_key_t k, size_t index, char table_type, int &node_nr, size_t nr_results) {
    int i = index;
    if (index == size) {
        i = 0;
    }

    do {
        if (table[i] != NULL) {
            if (table[i]->table_type == table_type && table[i]->k == k) {
                node_nr = table[i]->src;
                table[i]->table_type = 'U';
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
