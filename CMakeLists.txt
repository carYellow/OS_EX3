# cmake_minimum_required(VERSION <specify CMake version here>)
project(OS_EX3)

set(CMAKE_CXX_STANDARD 14)
SET(CMAKE_CXX_FLAGS -pthread)

add_executable(OS_EX3 MapReduceFramework.cpp Barrier/Barrier.cpp SampleClient.cpp)