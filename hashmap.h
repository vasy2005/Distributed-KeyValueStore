#ifndef HASH_MAP_H
#define HASH_MAP_H

#include <pthread.h>

typedef struct HashEntry
{
    int version;

    char* key;
    long long ttl;
    int value_len;

    int is_swapped;
    union 
    {  
        char* ram_value;
        long long disk_offset;
    } storage;

    int pending_eviction;

    struct HashEntry* nextEntry;
    struct HashEntry* prevEntry;

    struct HashEntry* lru_next;
    struct HashEntry* lru_prev;
} HashEntry;

typedef struct
{
    HashEntry* head;
    HashEntry* tail;
} LRUCache;

extern HashEntry** hashmap;
extern LRUCache lru_cache;
extern long long memory_used;

HashEntry** create_hashmap();
HashEntry* insert_in_hash(const char* key, const char* value, long long ttl);
HashEntry* get_from_key(const char* key);
void delete_from_hash(const char* key);

#endif
