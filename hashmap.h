#ifndef HASH_MAP_H
#define HASH_MAP_H

typedef struct HashEntry
{
    char* key;
    char* value;
    // int value_len;

    HashEntry* nextEntry;
    HashEntry* prevEntry;
} HashEntry;

#endif
