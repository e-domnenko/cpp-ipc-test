#pragma once

#include <atomic>
#include <mutex>
#include <condition_variable>
#include <thread>

namespace ipc_test::common
{
    class PausableWorker
    {
    public:
        explicit PausableWorker() {}

        void start()
        {
            running_ = true;
            worker_ = std::thread(
                [this]()
                { run_loop(); });
        }

        void stop()
        {
            running_ = false;
            resume();
            if (worker_.joinable())
                worker_.join();
        }

        void pause() { paused_ = true; }

        void resume()
        {
            {
                std::unique_lock lock(pause_mutex_);
                paused_ = false;
            }
            pause_cond_.notify_all();
        }

        bool toggle_pause()
        {
            {
                std::unique_lock lock(pause_mutex_);
                paused_ = !paused_;
            }

            if (!paused_)
            {
                pause_cond_.notify_all();
            }

            return paused_;
        }

    protected:
        virtual void run_loop() = 0;

        void pause_checkpoint()
        {
            std::unique_lock lock(pause_mutex_);
            while (paused_)
                pause_cond_.wait(lock);
        }

        bool stopped() { return !running_; }

    private:
        std::atomic_bool running_ = false;
        std::atomic_bool paused_ = false;

        std::mutex pause_mutex_;
        std::condition_variable pause_cond_;
        std::thread worker_;
    };
}
