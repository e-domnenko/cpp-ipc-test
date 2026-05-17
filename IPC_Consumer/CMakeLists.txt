add_executable(IPC_Consumer src/main.cpp)

target_link_libraries(IPC_Consumer PRIVATE IPC_Common)