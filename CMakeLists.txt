cmake_minimum_required(VERSION 3.30)
project(eglt-oss)

set(CMAKE_CXX_STANDARD 20)

set(EGLT_PROJECT_TOP_LEVEL ${CMAKE_CURRENT_SOURCE_DIR}/eglt)
set(EGLT_PROJECT_ROOT ${EGLT_PROJECT_TOP_LEVEL}/eglt)

add_library(eglt-oss SHARED ${EGLT_PROJECT_ROOT}/library.cpp)
