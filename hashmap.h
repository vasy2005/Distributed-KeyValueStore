#ifndef HASH_MAP_H
#define HASH_MAP_H

typedef struct HashEntry
{
    char* key;
    long long ttl;
    int value_len;

    int is_swapped;
    union 
    {  
        char* ram_value;
        long long disk_offset;
    } storage;
    

    HashEntry* nextEntry;
    HashEntry* prevEntry;
} HashEntry;

HashEntry** create_hashmap();
HashEntry* insert_in_hash(const char* key, const char* value, long long ttl);
HashEntry* get_from_hash(const char* key);
void delete_from_hash(const char* key);

#endif
