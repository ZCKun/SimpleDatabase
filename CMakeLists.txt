cmake_minimum_required(VERSION 3.20)
project(SimpleDatabase)
set(CMAKE_EXPORT_COMPILE_COMMANDS "ON")

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -Wall")

file(GLOB HEADER
    include
    )

include_directories(include)
add_executable(sd main.cpp ${HEADER})

