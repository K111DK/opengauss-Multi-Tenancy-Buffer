#ifndef LRU_H
#define LRU_H
#include <unordered_map>
#include <list>
#include <cstddef>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <cstdio>
#include "utils/dynahash.h"
#define DEFAULT_BUFFER_SIZE 1024
#define ACCESS_DEBUG_INTERVAL 1000
typedef struct tenant_buffer_cxt{
    pthread_mutex_t tenant_buffer_lock;
    std::string tenant_name;
    size_t capacity;
    uint64 total_access{0};
} tenant_buffer_cxt;

typedef struct tenant_info{
    pthread_mutex_t tenant_map_lock;
    struct HTAB* tenant_map;// tenant name -> tenant_buffer_cxt
} tenant_info;
#endif /* LRU_H */