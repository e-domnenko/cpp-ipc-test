#pragma once

#include <pthread.h>
#include <stdexcept>

namespace ipc_test::common::sync
{
    class ipc_mutex
    {
    public:
        ipc_mutex() : mutex_(nullptr) {};

        explicit ipc_mutex(pthread_mutex_t *mtx) : mutex_(mtx)
        {
            if (!mutex_)
            {
                throw std::invalid_argument("Mutex can not be null");
            }
        }

        ipc_mutex(const ipc_mutex& other) = default;
        ipc_mutex& operator=(const ipc_mutex& other) = default;

        ipc_mutex(ipc_mutex &&other) : mutex_(other.mutex_) {
            other.mutex_ = nullptr;
        }

        ipc_mutex& operator=(ipc_mutex &&other) {
            if (this != &other) {
                mutex_ = other.mutex_;
                other.mutex_ = nullptr;
            }

            return *this;
        }

        bool valid() const { return mutex_ != nullptr; }
        pthread_mutex_t* raw() { return mutex_; }

        void lock() const
        {
            if (!valid()) {
                throw std::runtime_error("ipc_mutex is not initialized");
            }

            if (pthread_mutex_lock(mutex_) != 0)
            {
                throw std::runtime_error("Failed to lock cross-proces mutex");
            }
        }

        void unlock() const
        {
            if (!valid()) {
                throw std::runtime_error("ipc_mutex is not initialized");
            }

            pthread_mutex_unlock(mutex_);
        }

    private:
        pthread_mutex_t *mutex_;
    };
}
