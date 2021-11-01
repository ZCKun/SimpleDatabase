cmake_minimum_required(VERSION 3.20)
project(SimpleDatabase)
set(CMAKE_EXPORT_COMPILE_COMMANDS "ON")

set(CMAKE_CXX_STANDARD 20)

add_executable(sd main.cpp table.h type.h)
