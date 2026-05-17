#pragma once

#include <pthread.h>
#include <cstdint>
#include <span>
#include <tuple>
#include <atomic>
#include "ipc_mutex.hpp"
#include "shared_memory_layout.hpp"

namespace ipc_test::common
{
    class ring_buffer_terminated : std::exception {};

    class SharedRingBuffer
    {
    public:
        SharedRingBuffer(void *shm, const size_t shm_size = 0) : shm_(reinterpret_cast<shared_memory_layout *>(shm))
        {
            initialize(shm_size);
        };

        SharedRingBuffer(const SharedRingBuffer &) = default;
        SharedRingBuffer &operator=(const SharedRingBuffer &) = default;

        SharedRingBuffer(SharedRingBuffer &&other) : shm_(other.shm_), mutex_(std::move(other.mutex_))
        {
            other.shm_ = nullptr;
        }

        SharedRingBuffer &operator=(SharedRingBuffer &&other)
        {
            if (this != &other)
            {
                shm_ = other.shm_;
                other.shm_ = nullptr;
                mutex_ = std::move(other.mutex_);
            }

            return *this;
        }

        static size_t required_size()
        {
            return sizeof(shared_memory_layout) + 2;
        }

        size_t free() const;
        size_t available() const;

        template <typename T>
        T read_value()
        {
            T value;
            size_t size = sizeof(T);
            read_value_bytes(reinterpret_cast<uint8_t *>(&value), size);
            return value;
        }

        const std::span<uint8_t> request_read(const size_t requested);
        void commit_read(const std::span<uint8_t> read);

        template <typename T>
        void write_value(const T &value)
        {
            size_t size = sizeof(T);
            write_value_bytes(reinterpret_cast<const uint8_t *>(&value), size);
        }

        std::span<uint8_t> request_write(const size_t requested);
        void commit_write(const std::span<uint8_t> written);

        void terminate();

    private:
        void initialize(const size_t shm_size = 0);
        size_t calc_available() const;
        size_t calc_free() const;
        void read_value_bytes(uint8_t *dest, const size_t size);
        void write_value_bytes(const uint8_t *src, const size_t size);

        shared_memory_layout *shm_;
        sync::ipc_mutex mutex_;
        std::atomic_bool terminated_ = false;
    };
}