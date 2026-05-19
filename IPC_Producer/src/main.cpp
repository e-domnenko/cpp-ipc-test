#include <sys/statvfs.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include <iostream>
#include <cstdint>
#include <limits>
#include <cerrno>
#include <cstring>
#include <ctime>
#include <random>
#include <algorithm>
#include <csignal>
#include <mutex>
#include <chrono>
#include <condition_variable>
#include <shared_memory_layout.hpp>
#include <SharedRingBuffer.hpp>
#include <PausableWorker.hpp>
#include <message_metadata.hpp>
#include <crc32.hpp>
#include <term_utils.hpp>
#include <shm_utils.hpp>

#include <termios.h>

static const size_t MESSAGE_QUEUE_SIZE = 100;
static const size_t DEFAULT_MESSAGE_SIZE = 256;

namespace ipc_test::producer
{
    rlim_t min_limit(rlim_t lima, rlim_t limb)
    {
        return lima == RLIM_INFINITY
                   ? limb
               : limb == RLIM_INFINITY
                   ? lima
                   : std::min(lima, limb);
    }

    size_t get_available_shm_size()
    {
        rlim_t rlimits_size = RLIM_INFINITY;
        struct rlimit limit;
        if (getrlimit(RLIMIT_MEMLOCK, &limit) == 0)
        {
            rlimits_size = min_limit(rlimits_size, limit.rlim_cur);
        }

        if (getrlimit(RLIMIT_AS, &limit) == 0)
        {
            rlimits_size = min_limit(rlimits_size, limit.rlim_cur);
        }

        if (getrlimit(RLIMIT_DATA, &limit) == 0)
        {
            rlimits_size = min_limit(rlimits_size, limit.rlim_cur);
        }

        uint64_t vfs_size = 0;
        struct statvfs vfs;
        if (statvfs("/dev/shm/", &vfs) == 0)
        {
            vfs_size = std::min(vfs.f_bavail * vfs.f_frsize, std::numeric_limits<size_t>::max());
        }

        return rlimits_size == RLIM_INFINITY ? vfs_size : std::min(vfs_size, rlimits_size);
    }

    int open_and_resize_shm(const size_t desired_size, size_t &shm_size, bool &is_new_shm)
    {
        auto shm_fd = shm_open(common::SHM_FILE_NAME, O_CREAT | O_RDWR, 0666);
        if (shm_fd == -1)
        {
            std::cerr << "Failed to open shared memory file /dev/shm" << common::SHM_FILE_NAME << " (" << std::strerror(errno) << ")" << std::endl;
            return -1;
        }

        struct stat fs;
        if (fstat(shm_fd, &fs) == 0)
        {
            if (fs.st_size == 0)
            {
                if (ftruncate(shm_fd, desired_size) == -1)
                {
                    std::cerr << "Failed to resize file: " << std::strerror(errno) << std::endl;
                    close(shm_fd);
                    shm_unlink(common::SHM_FILE_NAME);
                    return -1;
                }
                shm_size = desired_size;
                is_new_shm = true;
            }
            else
            {
                shm_size = fs.st_size;
            }
        }
        else
        {
            std::cerr << "Faild to access shared memory file" << std::endl;
            close(shm_fd);
            shm_unlink(common::SHM_FILE_NAME);
            return -1;
        }

        return shm_fd;
    }

    class IPCProducer : public common::PausableWorker
    {
    public:
        explicit IPCProducer(common::SharedRingBuffer &&ring_buffer, size_t message_size) : ring_buffer_(std::move(ring_buffer)), message_size_(message_size)
        {
            if (message_size_ == 0)
                throw std::invalid_argument("message_size can not be zero for IPCProducer");
        }

        void stop()
        {
            ring_buffer_.terminate();
            PausableWorker::stop();
        }

    protected:
        void run_loop()
        {
            std::random_device rd;
            std::mt19937 rng(rd());
            std::uniform_int_distribution<unsigned short> rnd_dist(0, 255);

            uint32_t sequence = 0;
            auto payload_size = sizeof(size_t) + message_size_ + sizeof(common::message_metadata);

            while (true)
            {
                pause_checkpoint();

                if (stopped())
                    break;

                size_t size_written = 0;
                size_t metadata_written = 0;
                size_t written = 0;
                uint32_t crc = 0;

                while (metadata_written < sizeof(common::message_metadata))
                {
                    if (stopped())
                        break;

                    auto writer = ring_buffer_.request_write(payload_size);

                    if (writer.empty())
                    {
                        break;
                    }

                    
                    size_written = writer.write_value(message_size_, size_written);
                    if (size_written < sizeof(size_t))
                    {
                        ring_buffer_.commit_write(writer);
                        continue;
                    }

                    while (written < message_size_)
                    {
                        auto chunk = writer.writer_chunk(message_size_ - written);
                        if (chunk.empty())
                        {
                            break;
                        }

                        std::generate(chunk.begin(), chunk.end(), [&rnd_dist, &rng]()
                                      { return static_cast<uint8_t>(rnd_dist(rng)); });
                        crc = common::crc32(chunk.data(), chunk.size(), crc);
                        written += chunk.size();
                    }

                    common::message_metadata msg_metadata{
                        .timestamp = std::time(nullptr),
                        .sequence = ++sequence,
                        .crc = crc,
                    };
                    metadata_written = writer.write_value(msg_metadata, metadata_written);

                    ring_buffer_.commit_write(writer);
                }

                std::this_thread::yield();
            }
        }

    private:
        common::SharedRingBuffer ring_buffer_;
        size_t message_size_;
    };
}

using namespace ipc_test;

int main(int argc, char *argv[])
{
    size_t message_size = 0;

    if (argc > 1)
    {
        auto parsed_size = std::atol(argv[1]);
        if (parsed_size <= 0)
        {
            std::cerr << "Invalid message size provided: " << argv[1] << std::endl;
            std::exit(1);
        }

        message_size = parsed_size;
        std::cout << "Starting producer with message size " << message_size << " bytes..." << std::endl;
    }
    else
    {
        message_size = DEFAULT_MESSAGE_SIZE;
        std::cout << "No message size provided resoting to default " << DEFAULT_MESSAGE_SIZE << std::endl;
    }

    auto shm_available = producer::get_available_shm_size();
    auto payload_size = sizeof(size_t) + message_size + sizeof(common::message_metadata);
    auto requested_size = payload_size * MESSAGE_QUEUE_SIZE + common::SharedRingBuffer::required_size();
    auto expected_shm_size = std::min(shm_available, requested_size);
    if (expected_shm_size < common::SharedRingBuffer::required_size())
    {
        std::cerr << "Not enough memory to allocate ring buffer (available: " << expected_shm_size << ", required: " << common::SharedRingBuffer::required_size() << ")" << std::endl;
        exit(1);
    }

    bool is_new_shm = false;
    size_t shm_size = 0;

    auto shm_fd = producer::open_and_resize_shm(expected_shm_size, shm_size, is_new_shm);
    if (shm_fd == -1)
        exit(1);

    void *shm_ptr = mmap(0, shm_size, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
    if (shm_ptr == MAP_FAILED)
    {
        std::cerr << "Failed to map shared memory file" << std::endl;
        close(shm_fd);
        shm_unlink(common::SHM_FILE_NAME);
        exit(1);
    }

    if (is_new_shm)
    {
        memset(shm_ptr, 0, shm_size);
    }
    else
    {
        auto creator_pid = reinterpret_cast<common::shared_memory_layout *>(shm_ptr)->creator_pid;
        if (creator_pid)
        {
            if (kill(creator_pid, 0) == 0 || errno == EPERM)
            {
                std::cerr << "Another producer process is already waiting for connections PID: " << creator_pid << std::endl;
                munmap(shm_ptr, shm_size);
                close(shm_fd);
                exit(2);
            }
        }

        munmap(shm_ptr, shm_size);
        close(shm_fd);
        shm_unlink(common::SHM_FILE_NAME);

        shm_fd = producer::open_and_resize_shm(expected_shm_size, shm_size, is_new_shm);
        if (shm_fd == -1)
            exit(1);

        void *shm_ptr = mmap(0, shm_size, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
        if (shm_ptr == MAP_FAILED)
        {
            std::cerr << "Failed to map shared memory file" << std::endl;
            close(shm_fd);
            shm_unlink(common::SHM_FILE_NAME);
            exit(1);
        }

        memset(shm_ptr, 0, shm_size);
    }

    if (mlock(shm_ptr, shm_size) == -1)
    {
        std::cerr << "Failed to map shared memory file" << std::endl;
        munmap(shm_ptr, shm_size);
        close(shm_fd);
        shm_unlink(common::SHM_FILE_NAME);
        exit(1);
    }

    reinterpret_cast<common::shared_memory_layout *>(shm_ptr)->creator_pid = getpid();

    std::cout << "Successfuly reserved " << shm_size << " bytes of shared memory" << std::endl;

    bool error_exit = false;

    try
    {
        common::KeyPressListener keyboard;

        producer::IPCProducer message_producer(common::SharedRingBuffer(shm_ptr, shm_size), message_size);
        message_producer.start(true);

        while (true)
        {
            char key = keyboard.get_key();

            if (!message_producer.running())
                break;

            if (key)
            {
                if (key == 'q')
                {
                    message_producer.stop();
                    break;
                }

                auto paused = message_producer.toggle_pause();
                if (paused)
                    std::cout << "Producer paused" << std::endl;
                else
                    std::cout << "Producer resumed" << std::endl;
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }
    catch (std::exception &exc)
    {
        error_exit = true;
        std::cerr << "Unknown error occured: " << exc.what() << std::endl;
    }

    munlock(shm_ptr, shm_size);
    munmap(shm_ptr, shm_size);
    close(shm_fd);
    shm_unlink(common::SHM_FILE_NAME);
    return error_exit ? 1 : 0;
}