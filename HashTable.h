#ifndef _HASHTABLE_H_
#define _HASHTABLE_H_

template <typename Record>
class HashTable {
public:
    //local HashTable for hash join
    //The HashTable stores keys and payloads in table R
    HashTable(size_t size) : num(size) {
        table = new record_r *[num];
        hash32_factor = 79;
    };

    size_t hash32(join_key_t k);
    int add(Record *r);
    int find(join_key_t k, Record **r, size_t index, size_t nr_results);	//index: starting searching index
    size_t erase(join_key_t k, size_t index, char table_type, int &node_nr, size_t nr_results);
    size_t getNextKey(size_t index, join_key_t &k);
    size_t getSize() {return size;}

private:
    size_t hash32_factor;
    size_t size;
    record_r **table;
};

template <typename Record>
size_t HashTable<Record>::hash32(join_key_t k) {
    uint64_t hash = (uint32_t) (k * hash32_factor);
    size_t res = (hash * size) >> 32;
    return res;
}

template <typename Record>
int HashTable<Record>::add(Record *r) {
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
            return -1;
        }
    }
}

template <typename Record>
int HashTable<Record>::find(join_key_t k, Record **r, size_t index, size_t nr_results) {
    size_t hash_key = hash32(k);
    size_t i = hash_key;

    if (index < size) {
        i = index;
    }

    do {
        if (table[i] != NULL && table[i]->k == k) {
            *r = table[i];
            return i;
        } else {
            i++;
        }
        if (i == size) {
            i = 0;
        }
    } while (table[i] != NULL && i != hash_key);

    return -1;
}

template <typename Record>
size_t HashTable<Record>::getNextKey(size_t index, join_key_t &k) {
    for (size_t i = index; i < size; i++) {
        if (table[i] != NULL) {
            k = table[i]->k;
            return i;
        }
    }

    return num;
}

template <typename Record>
size_t HashTable<Record>::erase(join_key_t k, size_t index, char table_type, int &node_nr, size_t nr_results) {

}

#endif
