#include "hashmap.h"
#define XXH_INLINE_ALL
#include "xxhash.h"
#include <stddef.h>

#define TABLE_SIZE 1024
#define SEED 1000000009
//power of 2

HashEntry** hashmap;

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
    XXH64_hash_t hash = XX64(key, strlen(key), SEED);
    long long int index = hash & (TABLE_SIZE-1);
    return index;
}

HashEntry* insert(const char* key, const char* value)
{
    long long int index = get_hash_from_key(key);
    for (HashEntry* it = hashmap[index]; it != NULL; it = it->nextEntry)
        if (strcmp(it->key, key) == 0)
        {
            free(it->value);
            it->value = strdup(value);
            return it;
        }

    HashEntry* hashEntry = malloc(sizeof(HashEntry));
    if (hashEntry == NULL)
        return NULL;
    
    hashEntry->key = strdup(key);
    hashEntry->value = strdup(value);

    hashEntry->nextEntry = hashmap[index];
    hashEntry->prevEntry = NULL;

    if (hashmap[index] != NULL)
        hashmap[index]->prevEntry = hashEntry;
    hashmap[index] = hashEntry;

    return hashEntry;
}

HashEntry* get(const char* key)
{
    long long index = get_hash_from_key(key);
    for (HashEntry* it = hashmap[index]; it != NULL; it = it->nextEntry)
        if (strcmp(it->key, key) == 0)
            return it;

    return NULL;
}

void delete(const char* key)
{
    long long index = get_hash_from_key(key);
    for (HashEntry* it = hashmap[index]; it != NULL; it = it->nextEntry)
        if (strcmp(it->key, key) == 0)
        {
            free(it->key); free(it->value);

            if (it->prevEntry != NULL)
                it->prevEntry->nextEntry = it->nextEntry;
            else
                hashmap[index] = it->nextEntry;

            if (it->nextEntry != NULL)
                it->nextEntry->prevEntry = it->prevEntry;

            free(it);
            break;
        }
}

