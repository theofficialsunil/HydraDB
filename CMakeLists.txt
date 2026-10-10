cmake_minimum_required(VERSION 3.20)

project(HydraDB
    VERSION 0.1.0
    DESCRIPTION "Distributed storage and data recovery system"
    LANGUAGES CXX
)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_executable(hydra
    src/main.cpp
    src/network/tcp_server.cpp
    src/network/tcp_client.cpp
    src/cluster/cluster_manager.cpp
    src/sharding/shard_manager.cpp
)

target_include_directories(hydra PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
)

target_compile_options(hydra PRIVATE
    -Wall
    -Wextra
    -Wpedantic
)

if(WIN32)
    target_link_libraries(hydra PRIVATE ws2_32)
endif()