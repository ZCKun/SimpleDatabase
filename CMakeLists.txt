cmake_minimum_required(VERSION 3.20)
project(SimpleDatabase)

set(CMAKE_CXX_STANDARD 23)

add_executable(SimpleDatabase main.cpp table.h type.h)
