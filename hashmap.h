#include "entry.h"

struct HashEntry
{
    char* key;
    Entry* value;
    HashEntry* nextEntry;
    HashEntry* prevEntry;
};

class Hashmap
{
private:
    HashEntry* entry;
    

};