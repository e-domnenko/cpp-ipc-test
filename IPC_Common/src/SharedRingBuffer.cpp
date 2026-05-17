#include <mutex>
#include <cstring>
#include "SharedRingBuffer.hpp"

using namespace ipc_test::common;

void SharedRingBuffer::initialize(const size_t shm_size)
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

size_t SharedRingBuffer::calc_available() const
{
    if (shm_->head == shm_->tail)
        return shm_->full ? shm_->capacity : 0;
    return ((shm_->tail - shm_->head) + shm_->capacity) % shm_->capacity;
}

size_t SharedRingBuffer::available() const
{
    std::lock_guard lock(mutex_);
    return calc_available();
}

void SharedRingBuffer::read_value_bytes(uint8_t *dest, const size_t size)
{
    size_t total_read = 0;
    while (size > total_read)
    {
        auto chunk = request_read(size - total_read);
        auto read = chunk.size();
        memcpy(dest, chunk.data(), read);
        dest += read;
        total_read += read;
        commit_read(chunk);
    }
}

void SharedRingBuffer::commit_read(const std::span<uint8_t> read)
{
    if (read.size() == 0)
        return;

    auto size_read = read.size();
    std::lock_guard lock(mutex_);
    auto avail_before = calc_available();
    if (avail_before < size_read)
        throw std::invalid_argument("Can not advance past tail");

    shm_->head = (shm_->head + size_read) % shm_->capacity;
    shm_->full = false;

    pthread_cond_signal(&shm_->consumer_cond);
}

const std::span<uint8_t> SharedRingBuffer::request_read(const size_t requested)
{
    if (requested == 0)
        throw std::invalid_argument("requested can not be zero");

    if (terminated_)
        throw ring_buffer_terminated();

    std::lock_guard lock(mutex_);
    auto available = calc_available();
    while (available == 0 && !terminated_)
    {
        pthread_cond_wait(&shm_->producer_cond, mutex_.raw());
        if (terminated_)
            throw ring_buffer_terminated();
        available = calc_available();
    }

    auto actual = std::min(std::min(available, shm_->capacity - shm_->head), requested);
    return std::span<uint8_t>(&shm_->buffer[shm_->head], actual);
}

size_t SharedRingBuffer::calc_free() const
{
    if (shm_->head == shm_->tail)
        return shm_->full ? 0 : shm_->capacity;
    return (shm_->capacity - (shm_->tail - shm_->head)) % shm_->capacity;
}

size_t SharedRingBuffer::free() const
{
    std::lock_guard lock(mutex_);
    return calc_free();
}

void SharedRingBuffer::write_value_bytes(const uint8_t *src, const size_t size)
{
    size_t total_written = 0;
    while (size > total_written)
    {
        auto chunk = request_write(size - total_written);
        auto written = chunk.size();
        std::memcpy(chunk.data(), src, written);
        src += written;
        total_written += written;
        commit_write(chunk);
    }
}

std::span<uint8_t> SharedRingBuffer::request_write(const size_t requested)
{
    if (requested == 0)
        throw std::invalid_argument("requested can not be zero");

    if (terminated_)
        throw ring_buffer_terminated();

    std::lock_guard lock(mutex_);
    auto free = calc_free();
    while (free == 0)
    {
        pthread_cond_wait(&shm_->consumer_cond, mutex_.raw());
        if (terminated_)
            throw ring_buffer_terminated();

        free = calc_free();
    }

    auto reserved = std::min(std::min(free, shm_->capacity - shm_->tail), requested);
    return std::span<uint8_t>(&shm_->buffer[shm_->tail], reserved);
}

void SharedRingBuffer::commit_write(const std::span<uint8_t> written)
{
    if (written.size() == 0)
        return;

    auto size_written = written.size();
    std::lock_guard lock(mutex_);
    auto free_before = calc_free();
    if (free_before < size_written)
        throw std::invalid_argument("Can not advance post head");

    shm_->tail = (shm_->tail + size_written) % shm_->capacity;
    shm_->full = free_before - size_written == 0;
    pthread_cond_signal(&shm_->producer_cond);
}

void SharedRingBuffer::terminate()
{
    terminated_ = true;
    pthread_cond_broadcast(&shm_->producer_cond);
    pthread_cond_broadcast(&shm_->consumer_cond);
}
