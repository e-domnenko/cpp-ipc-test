#pragma once

#include <cstdint>

namespace ipc_test::common {
    struct message_metadata
    {
        int64_t timestamp;
        uint32_t sequence;
        uint32_t crc;
    };   
}