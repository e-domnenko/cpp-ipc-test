#pragma once

#include <pthread.h>
#include <cstdint>

namespace ipc_test::common
{
    struct shared_memory_layout
    {
        long creator_pid;
        pthread_mutex_t mutex;
        pthread_cond_t producer_cond;
        pthread_cond_t consumer_cond;

        size_t capacity;
        size_t head;
        size_t tail;
        bool full;

        bool initialized;

        uint8_t buffer[];
    };
}