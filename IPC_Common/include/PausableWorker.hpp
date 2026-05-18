#pragma once

#include <atomic>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <pthread.h>
#include <sched.h>
#include <sys/resource.h>

namespace ipc_test::common
{
    class PausableWorker
    {
    public:
        static constexpr int ELEVATED_PRIORITY = 80;

        explicit PausableWorker() {}

        void start(bool elevate = false)
        {
            running_ = true;
            worker_ = std::thread(
                [this]()
                { run_loop(); });

            if (elevate)
            {
                int max_prio = 0;
                if (getuid() == 0)
                {
                    max_prio = sched_get_priority_max(SCHED_FIFO);
                }
                else
                {
                    struct rlimit lim{};
                    if (getrlimit(RLIMIT_RTPRIO, &lim) != 0)
                        lim.rlim_cur = RLIM_INFINITY;

                    max_prio = lim.rlim_cur == RLIM_INFINITY
                                   ? sched_get_priority_max(SCHED_FIFO)
                                   : lim.rlim_cur;
                }

                int selected_prio = std::min(ELEVATED_PRIORITY, max_prio);

                bool is_elevated = false;
                if (selected_prio > 0)
                {
                    sched_param sch{.sched_priority = selected_prio};
                    if (pthread_setschedparam(worker_.native_handle(), SCHED_FIFO, &sch) == 0)
                        is_elevated = true;
                }

                if (!is_elevated)
                {
                    std::cerr << "Warning: could not set RT priority, running best-effort" << std::endl;
                }
            }
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
