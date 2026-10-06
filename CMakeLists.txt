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
)

target_compile_options(hydra PRIVATE
    -Wall
    -Wextra
    -Wpedantic
)