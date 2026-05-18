#include <iostream>
#include <iomanip>
#include <chrono>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>
#include <crc32.hpp>
#include <SharedRingBuffer.hpp>
#include <shm_utils.hpp>
#include <message_metadata.hpp>
#include <PausableWorker.hpp>
#include <term_utils.hpp>

namespace ipc_test::consumer
{
    std::string humanize_througput(float througput)
    {
        static constexpr std::string_view size_prefixes = "KMGT";
        char prefix = 0;
        for (char current_prefix : size_prefixes)
        {
            float candidate_throughput = througput / 1024;
            if (candidate_throughput < 1)
                break;
            througput = candidate_throughput;
            prefix = current_prefix;
        }

        if (prefix)
            return std::format("{:.1f} {}B/s", througput, prefix);

        return std::format("{:.1f} B/s", througput);
    }

    class IPCConsumer : public common::PausableWorker
    {
    public:
        explicit IPCConsumer(common::SharedRingBuffer &&ring_buffer) : ring_buffer_(std::move(ring_buffer)) {}

        void stop()
        {
            ring_buffer_.terminate();
            PausableWorker::stop();
        }

    protected:
        void run_loop()
        {
            auto round_start_time = std::chrono::steady_clock::now();
            uint32_t messages_read = 0;
            uint64_t total_messages_read = 0;
            uint32_t invalid_crc = 0;
            uint32_t first_sequnce_num = 0;
            uint32_t cumulative_message_size = 0;

            while (true)
            {
                pause_checkpoint();

                if (stopped())
                    break;

                try
                {
                    auto message_size = ring_buffer_.read_value<size_t>();

                    uint32_t crc = 0;
                    size_t read = 0;
                    while (read < message_size)
                    {
                        auto chunk = ring_buffer_.request_read(message_size - read);
                        crc = common::crc32(chunk.data(), chunk.size(), crc);
                        read += chunk.size();
                        ring_buffer_.commit_read(chunk);
                    }

                    auto message_metadata = ring_buffer_.read_value<common::message_metadata>();
                    messages_read++;
                    total_messages_read++;
                    cumulative_message_size += message_size;
                    if (crc != message_metadata.crc)
                        invalid_crc++;
                    if (!first_sequnce_num)
                        first_sequnce_num = message_metadata.sequence;

                    auto elapsed = std::chrono::steady_clock::now() - round_start_time;
                    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();
                    if (elapsed_ms > 1000)
                    {
                        auto system_now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
                        std::cout << std::ctime(&system_now)
                                  << "Processed " << total_messages_read << " messages" << std::endl
                                  << static_cast<size_t>(static_cast<float>(messages_read) / elapsed_ms * 1000) << " messages/s; "
                                  << consumer::humanize_througput(static_cast<float>(cumulative_message_size) / elapsed_ms * 1000) << std::endl
                                  << "Sequnce numbers from " << first_sequnce_num << " to " << message_metadata.sequence << std::endl
                                  << "Messages with invalid CRC: " << invalid_crc << std::endl
                                  << std::endl;

                        round_start_time = std::chrono::steady_clock::now();
                        messages_read = 0;
                        first_sequnce_num = 0;
                        invalid_crc = 0;
                        cumulative_message_size = 0;
                    }
                }
                catch (common::ring_buffer_terminated)
                {
                    break;
                }
            }
        }

    private:
        common::SharedRingBuffer ring_buffer_;
    };
}

using namespace ipc_test;

int main()
{
    auto shm_fd = shm_open(common::SHM_FILE_NAME, O_RDWR, 0666);
    if (shm_fd == -1)
    {
        if (errno == ENOENT)
            std::cerr << "Shared memory file does not exist. Run procuder process first." << std::endl;
        else
            std::cerr << "Failed to open shared memory file /dev/shm" << common::SHM_FILE_NAME << " (" << std::strerror(errno) << ")" << std::endl;
        return 1;
    }

    size_t shm_size = 0;
    struct stat fs;
    if (fstat(shm_fd, &fs) == 0)
    {
        shm_size = fs.st_size;
        if (shm_size < common::SharedRingBuffer::required_size())
        {
            std::cout << "Shared memory size is too small. Probably corrupted or created by different processs." << std::endl;
            exit(1);
        }
    }
    else
    {
        std::cerr << "Faild to access shared memory file" << std::endl;
        close(shm_fd);
        return -1;
    }

    void *shm_ptr = mmap(0, shm_size, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
    if (shm_ptr == MAP_FAILED)
    {
        std::cerr << "Failed to map shared memory file" << std::endl;
        close(shm_fd);
        exit(1);
    }

    try
    {
        common::KeyPressListener keyboard;

        consumer::IPCConsumer message_consumer(common::SharedRingBuffer(shm_ptr, shm_size));

        // Unlink the file to prevent leaking if producer is killed.
        shm_unlink(common::SHM_FILE_NAME);

        message_consumer.start(true);

        while (true)
        {
            char key = keyboard.get_key();

            if (key)
            {
                if (key == 'q')
                {
                    message_consumer.stop();
                    break;
                }

                auto paused = message_consumer.toggle_pause();
                if (paused)
                    std::cout << "Consumer paused" << std::endl;
                else
                    std::cout << "Consumer resumed" << std::endl;
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }
    catch (const std::exception &e)
    {
        std::cerr << e.what() << std::endl;
    }

    munmap(shm_ptr, shm_size);
    close(shm_fd);
    shm_unlink(common::SHM_FILE_NAME);
}