#pragma once

#include <pthread.h>
#include <cstdint>
#include <span>
#include <tuple>
#include <atomic>
#include <mutex>
#include "ipc_mutex.hpp"
#include "shared_memory_layout.hpp"

namespace ipc_test::common
{
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

        size_t free() const
        {
            std::lock_guard lock(mutex_);
            return calc_free();
        };

        size_t available() const
        {
            std::lock_guard lock(mutex_);
            return calc_available();
        }

        void terminate()
        {
            terminated_ = true;
            pthread_cond_broadcast(&shm_->producer_cond);
            pthread_cond_broadcast(&shm_->consumer_cond);
        }

    private:
        class ChunksIterator
        {
        public:
            bool empty() const { return buffer_ == nullptr || (position_ == tail_ && !full_); }

            size_t size() const
            {
                if (buffer_ == nullptr)
                    return 0;
                auto sz = ((tail_ - head_) + capacity_) % capacity_;
                return sz == 0 ? (full_ ? capacity_ : 0) : sz;
            }

            size_t processed() const
            {
                if (buffer_ == nullptr)
                    return 0;
                auto sz = ((position_ - head_) + capacity_) % capacity_;
                return sz == 0 ? (done_ ? capacity_ : 0) : sz;
            }

            size_t remains() const
            {
                if (buffer_ == nullptr)
                    return 0;
                auto sz = ((tail_ - position_) + capacity_) % capacity_;
                return sz == 0 ? (position_ == head_ && full_ && !done_ ? capacity_ : 0) : sz;
            }

            void skip(size_t size)
            {
                if (remains() < size)
                    throw std::range_error("Cannot skip more than allocated");
                position_ = (position_ + size) % capacity_;
            }

            ChunksIterator() : head_(0), tail_(0), position_(0), capacity_(0), full_(false), buffer_(nullptr) {};

            ChunksIterator(uint8_t *buffer, size_t head, size_t tail, size_t capacity, bool full)
                : head_(head), tail_(tail), position_(head), capacity_(capacity), full_(full), buffer_(buffer)
            {
            }

        protected:
            std::span<uint8_t> contigious_chunk(size_t size)
            {
                auto max_size = remains();
                size = std::min(size, max_size);
                if (size == 0 || empty())
                    return std::span<uint8_t>();

                auto prev_position = position_;
                auto next_position = position_ + size;
                position_ = next_position < capacity_ ? next_position : 0;
                done_ = position_ == tail_;

                return std::span<uint8_t>(&buffer_[prev_position], position_ == 0 ? capacity_ - prev_position : size);
            }

        private:
            size_t head_;
            size_t tail_;
            size_t position_;
            size_t capacity_;
            bool full_;
            bool done_ = false;
            uint8_t *buffer_;
        };

    public:
        class Writer : public ChunksIterator
        {
        public:
            template <typename T>
            size_t write_value(const T &value, ptrdiff_t offset = 0)
            {
                size_t size = sizeof(T);
                if (offset >= size)
                    return size;

                auto bytes = reinterpret_cast<const uint8_t *>(&value);
                bytes += offset;
                size_t written = offset;
                while (written < size)
                {
                    auto chunk = writer_chunk(size - written);
                    if (chunk.empty())
                        return written;

                    memcpy(chunk.data(), bytes, chunk.size());
                    written += chunk.size();
                    bytes += written;
                }

                return written;
            }

            std::span<uint8_t> writer_chunk(size_t size)
            {
                return contigious_chunk(size);
            }

        private:
            friend class SharedRingBuffer;

            Writer() : ChunksIterator() {}

            Writer(uint8_t *buffer, size_t head, size_t tail, size_t capacity, bool full)
                : ChunksIterator(buffer, head, tail, capacity, full) {};
        };

        class Reader : public ChunksIterator
        {
        public:
            template <typename T>
            size_t read_value(T &value, ptrdiff_t offset = 0)
            {
                auto size = sizeof(T);
                if (offset >= size)
                    return size;

                auto bytes = reinterpret_cast<uint8_t *>(&value);
                bytes += offset;
                size_t read = offset;
                while (read < size)
                {
                    auto chunk = reader_chunk(size - read);
                    if (chunk.empty())
                        return read;

                    memcpy(bytes, chunk.data(), chunk.size());
                    read += chunk.size();
                    bytes += read;
                }

                return read;
            }

            const std::span<uint8_t> reader_chunk(size_t size)
            {
                return contigious_chunk(size);
            }

        private:
            friend class SharedRingBuffer;

            Reader() : ChunksIterator() {}

            Reader(uint8_t *buffer, size_t head, size_t tail, size_t capacity, bool full)
                : ChunksIterator(buffer, head, tail, capacity, full) {};
        };

        Writer request_write(size_t requested)
        {
            if (requested == 0)
                throw std::invalid_argument("requested can not be zero");

            if (terminated_)
                return Writer();

            if (requested > shm_->capacity)
                requested = shm_->capacity;

            std::lock_guard lock(mutex_);
            auto free = calc_free();
            while (free < requested)
            {
                pthread_cond_wait(&shm_->consumer_cond, mutex_.raw());
                if (terminated_)
                    return Writer();

                free = calc_free();
            }

            return Writer(shm_->buffer, shm_->tail, (shm_->tail + requested) % shm_->capacity, shm_->capacity, !shm_->full);
        }

        void commit_write(const Writer &writer)
        {
            if (writer.size() == 0)
                return;

            auto size_written = writer.processed();
            std::lock_guard lock(mutex_);
            auto free_before = calc_free();
            if (free_before < size_written)
                throw std::invalid_argument("Can not advance post head");

            shm_->tail = (shm_->tail + size_written) % shm_->capacity;
            shm_->full = free_before - size_written == 0;
            pthread_cond_signal(&shm_->producer_cond);
        }

        Reader request_read(size_t requested)
        {
            if (requested == 0)
                throw std::invalid_argument("requested can not be zero");

            if (terminated_)
                return Reader();

            if (requested > shm_->capacity)
                requested = shm_->capacity;

            std::lock_guard lock(mutex_);
            auto available = calc_available();
            while (available < requested)
            {
                pthread_cond_wait(&shm_->producer_cond, mutex_.raw());
                if (terminated_)
                    return Reader();

                available = calc_available();
            }

            return Reader(shm_->buffer, shm_->head, (shm_->head + requested) % shm_->capacity, shm_->capacity, shm_->full);
        }

        void commit_read(const Reader &reader)
        {
            if (reader.size() == 0)
                return;

            auto size_read = reader.processed();
            std::lock_guard lock(mutex_);
            auto available_before = calc_available();
            if (available_before < size_read)
                throw std::invalid_argument("Can not advance post head");

            shm_->head = (shm_->head + size_read) % shm_->capacity;
            shm_->full = false;
            pthread_cond_signal(&shm_->consumer_cond);
        }

    private:
        void initialize(const size_t shm_size = 0)
        {
            if (shm_->initialized)
            {
                auto mtxret = pthread_mutex_trylock(&shm_->mutex);
                if (mtxret != EINVAL)
                {
                    mutex_ = sync::ipc_mutex(&shm_->mutex);
                    if (mtxret == 0)
                        pthread_mutex_unlock(&shm_->mutex);
                    return;
                }
            }

            if (shm_size == 0)
            {
                throw std::invalid_argument("shm_size is not provided for uninitilized shared memory");
            }

            auto header_size = sizeof(shared_memory_layout);
            if (shm_size <= header_size)
            {
                throw std::length_error("Not enough shared memory allocated");
            }

            shm_->capacity = shm_size - header_size;
            shm_->head = 0;
            shm_->tail = 0;
            shm_->full = false;

            int error_code = 0;
            pthread_mutexattr_t mtx_attr;
            pthread_mutexattr_init(&mtx_attr);
            pthread_mutexattr_setpshared(&mtx_attr, PTHREAD_PROCESS_SHARED);
            error_code = pthread_mutex_init(&shm_->mutex, &mtx_attr);
            pthread_mutexattr_destroy(&mtx_attr);
            if (error_code != 0)
                throw std::runtime_error("Failed to create shared mutex");

            pthread_condattr_t p_cond_attr;
            pthread_condattr_init(&p_cond_attr);
            pthread_condattr_setpshared(&p_cond_attr, PTHREAD_PROCESS_SHARED);
            error_code = pthread_cond_init(&shm_->producer_cond, &p_cond_attr);
            pthread_condattr_destroy(&p_cond_attr);
            if (error_code != 0)
            {
                pthread_mutex_destroy(&shm_->mutex);
                throw std::runtime_error("Failed to create producer cond");
            }

            pthread_condattr_t c_cond_attr;
            pthread_condattr_init(&c_cond_attr);
            pthread_condattr_setpshared(&c_cond_attr, PTHREAD_PROCESS_SHARED);
            error_code = pthread_cond_init(&shm_->consumer_cond, &c_cond_attr);
            pthread_condattr_destroy(&c_cond_attr);
            if (error_code != 0)
            {
                pthread_cond_destroy(&shm_->producer_cond);
                pthread_mutex_destroy(&shm_->mutex);
                throw std::runtime_error("Failed to create consumer cond");
            }

            shm_->initialized = true;
            mutex_ = sync::ipc_mutex(&shm_->mutex);
        }

        size_t calc_available() const
        {
            if (shm_->head == shm_->tail)
                return shm_->full ? shm_->capacity : 0;
            return ((shm_->tail - shm_->head) + shm_->capacity) % shm_->capacity;
        }

        size_t calc_free() const
        {
            if (shm_->head == shm_->tail)
                return shm_->full ? 0 : shm_->capacity;
            return (shm_->capacity - (shm_->tail - shm_->head)) % shm_->capacity;
        }

        shared_memory_layout *shm_;
        sync::ipc_mutex mutex_;
        std::atomic_bool terminated_ = false;
    };
}