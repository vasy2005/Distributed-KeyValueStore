#include "hashmap.h"
#define XXH_INLINE_ALL
#include "xxhash.h"
#include <stddef.h>
#include <stdio.h>

//power of 2

HashEntry** hashmap;
LRUCache lru_cache;
long long memory_used = 0;
long long global_version_counter = 0;

//LRUCache
void lru_init()
{
    lru_cache.head = NULL;
    lru_cache.tail = NULL;
}

void lru_promote(HashEntry* entry)
{
    if (entry == lru_cache.head)
        return;

    if (entry->lru_prev != NULL)
        entry->lru_prev->lru_next = entry->lru_next;
    if (entry->lru_next != NULL)
        entry->lru_next->lru_prev = entry->lru_prev;

    if (lru_cache.tail == entry)
        lru_cache.tail = entry->lru_prev;

    entry->lru_next = lru_cache.head;

    if (lru_cache.head != NULL)
        lru_cache.head->lru_prev = entry;

    lru_cache.head = entry;

    if (lru_cache.tail == NULL)
        lru_cache.tail = lru_cache.head;

    entry->lru_prev = NULL;
}

void lru_remove(HashEntry* entry)
{
    if (entry->lru_prev != NULL)
        entry->lru_prev->lru_next = entry->lru_next;
    else
        lru_cache.head = entry->lru_next;
    if (entry->lru_next != NULL)
        entry->lru_next->lru_prev = entry->lru_prev;
    else
        lru_cache.tail = entry->lru_prev; 

    entry->lru_next = entry->lru_prev = NULL;
}

//HashMap
HashEntry** create_hashmap()
{
    hashmap = (HashEntry**)malloc(TABLE_SIZE * sizeof(HashEntry*));
    if (hashmap != NULL)
        for (int i = 0; i < TABLE_SIZE; i++)
            hashmap[i] = NULL;

    return hashmap;
}

long long int get_hash_from_key(const char* key)
{
    XXH64_hash_t hash = XXH64(key, strlen(key), SEED);
    long long int index = hash & (TABLE_SIZE-1);
    return index;
}

HashEntry* insert_in_hash(const char* key, const char* value, long long ttl)
{
    long long int index = get_hash_from_key(key);
    for (HashEntry* it = hashmap[index]; it != NULL; it = it->nextEntry)
        if (strcmp(it->key, key) == 0)
        {
            if (it->is_swapped == 0)
            {
                free(it->storage.ram_value);
                memory_used -= it->value_len+1;
            }
            
            it->expiry_time = time(NULL) + ttl;
            it->storage.ram_value = strdup(value);
            it->value_len = strlen(value);
            it->is_swapped = 0;
            it->version = ++global_version_counter;

            memory_used += it->value_len+1;
            return it;
        }

    HashEntry* hashEntry = malloc(sizeof(HashEntry));
    if (hashEntry == NULL)
        return NULL;
    
    hashEntry->expiry_time = time(NULL) + ttl;
    hashEntry->key = strdup(key);
    hashEntry->storage.ram_value = strdup(value);
    hashEntry->value_len = strlen(value);
    hashEntry->version = ++global_version_counter;
    hashEntry->pending_eviction = 0;
    hashEntry->is_swapped = 0;

    memory_used += strlen(hashEntry->key) + 1;
    memory_used += hashEntry->value_len + 1;
    memory_used += sizeof(HashEntry);

    hashEntry->nextEntry = hashmap[index];
    hashEntry->prevEntry = NULL;

    hashEntry->lru_next = hashEntry->lru_prev = NULL;

    if (hashmap[index] != NULL)
        hashmap[index]->prevEntry = hashEntry;
    hashmap[index] = hashEntry;

    return hashEntry;
}

HashEntry* get_from_key(const char* key)
{
    long long index = get_hash_from_key(key);
    for (HashEntry* it = hashmap[index]; it != NULL; it = it->nextEntry)
        if (strcmp(it->key, key) == 0)
        {
            if (time(NULL) > it->expiry_time)
            {
                delete_from_hash(key);
                printf("Elementul cu key %s a expirat\n", it->key); fflush(stdout);
                return NULL;
            }
            return it;
        }

    return NULL;
}

int delete_from_hash(const char* key)
{
    long long index = get_hash_from_key(key);
    for (HashEntry* it = hashmap[index]; it != NULL; it = it->nextEntry)
        if (strcmp(it->key, key) == 0)
        {
            memory_used -= sizeof(HashEntry);
            memory_used -= strlen(it->key) + 1;
            
            if (it->is_swapped == 0)
            {
                memory_used -= it->value_len+1;
                free(it->storage.ram_value);
                lru_remove(it);
            }
            free(it->key);
            
            if (it->prevEntry != NULL)
                it->prevEntry->nextEntry = it->nextEntry;
            else
                hashmap[index] = it->nextEntry;

            if (it->nextEntry != NULL)
                it->nextEntry->prevEntry = it->prevEntry;


            free(it);
            return 0;
        }
    return -1;
}
