#pragma once

#include <unistd.h>
#include <termios.h>
#include <atomic>

namespace ipc_test::common
{
    class KeyPressListener
    {
    public:
        KeyPressListener()
        {
            if (!active_count_++)
            {
                struct termios new_term;

                if (tcgetattr(STDIN_FILENO, &old_term_) == 0)
                {
                    new_term = old_term_;
                    new_term.c_lflag &= ~(ICANON | ECHO);
                    new_term.c_cc[VMIN] = 1;
                    new_term.c_cc[VTIME] = 0;

                    tcsetattr(STDIN_FILENO, TCSANOW, &new_term);
                }
                else
                {
                    active_count_--;
                }
            }
        }

        ~KeyPressListener()
        {
            if (!--active_count_)
            {
                tcsetattr(STDIN_FILENO, TCSANOW, &old_term_);
            }
        }

        char get_key()
        {
            char ch = 0;
            if (read(STDIN_FILENO, &ch, 1) < 0)
                ch = 0;

            return ch;
        }

    private:
        inline static std::atomic_int active_count_{0};
        inline static struct termios old_term_{};
    };
}