add_executable(IPC_Producer src/main.cpp)

target_link_libraries(IPC_Producer PRIVATE IPC_Common)
