#pragma once
#include <cstdint>
#include <memory>

#ifdef USE_ZLIB
#include <zlib.h>
#else
#include <CRC.h>
#endif

namespace ipc_test::common
{
    namespace _internal
    {
#ifndef USE_ZLIB
        CRC::Table<uint32_t, 32> crc32lut(CRC::CRC_32());
#endif
    }

    inline uint32_t crc32(const void *data, size_t len, uint32_t crc = 0)
    {
#ifdef USE_ZLIB
        // TODO: Use zlibs crc32
        return ::crc32(crc, static_cast<const uint8_t*>(data), len);
#else
        return CRC::Calculate(data, len, _internal::crc32lut, crc);
#endif
    }
}