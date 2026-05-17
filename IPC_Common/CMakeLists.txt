add_library(IPC_Common STATIC
    src/SharedRingBuffer.cpp)

target_include_directories(IPC_Common PUBLIC include)

find_package(ZLIB MODULE QUIET)

if (ZLIB_FOUND)
    target_link_libraries(IPC_Common PUBLIC ZLIB::ZLIB)
    target_compile_definitions(IPC_Common PUBLIC USE_ZLIB)
else()
    set(CRCPP_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third_party/CRCpp/inc")
    target_include_directories(IPC_Common SYSTEM PUBLIC $<BUILD_INTERFACE:${CRCPP_INCLUDE_DIR}>)
endif()
