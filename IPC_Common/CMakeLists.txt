add_library(IPC_Common INTERFACE)

target_include_directories(IPC_Common INTERFACE include)

find_package(ZLIB MODULE QUIET)

if (ZLIB_FOUND)
    target_link_libraries(IPC_Common INTERFACE ZLIB::ZLIB)
    target_compile_definitions(IPC_Common INTERFACE USE_ZLIB)
else()
    set(CRCPP_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third_party/CRCpp/inc")
    target_include_directories(IPC_Common SYSTEM INTERFACE $<BUILD_INTERFACE:${CRCPP_INCLUDE_DIR}>)
endif()
