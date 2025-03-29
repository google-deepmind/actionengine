cmake_minimum_required(VERSION 3.30)
project(eglt-oss)

set(CMAKE_CXX_STANDARD 20)

set(EGLT_PROJECT_TOP_LEVEL ${CMAKE_CURRENT_SOURCE_DIR}/eglt)
set(EGLT_PROJECT_ROOT ${EGLT_PROJECT_TOP_LEVEL}/eglt)

set(EGLT_THIRD_PARTY_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/third_party)
set(EGLT_THIRD_PARTY_INSTALL_ROOT ${EGLT_THIRD_PARTY_ROOT}/build_deps)

set(CMAKE_PREFIX_PATH
    ${CMAKE_PREFIX_PATH}
    ${EGLT_THIRD_PARTY_INSTALL_ROOT}/abseil-cpp
    ${EGLT_THIRD_PARTY_INSTALL_ROOT}/pybind11
    ${EGLT_THIRD_PARTY_INSTALL_ROOT}/boost
    ${EGLT_THIRD_PARTY_INSTALL_ROOT}/fiber
    ${EGLT_THIRD_PARTY_INSTALL_ROOT}/googletest
    ${EGLT_THIRD_PARTY_INSTALL_ROOT}/poco
)

execute_process(
    COMMAND python3 -c "import sys; print(sys.executable, end='')"
    OUTPUT_VARIABLE Python_EXECUTABLE
)
find_package(Python REQUIRED COMPONENTS Development Interpreter)

set(pybind11_DIR ${EGLT_THIRD_PARTY_INSTALL_ROOT}/pybind11/lib/cmake/pybind11)
find_package(pybind11 REQUIRED)

find_package(GTest REQUIRED)

#set(absl_DIR ${EGLT_THIRD_PARTY_INSTALL_ROOT}/abseil-cpp/lib/cmake/absl)
find_package(absl REQUIRED)
find_package(Boost REQUIRED COMPONENTS fiber)
find_package(GTest REQUIRED)
find_package(Poco REQUIRED COMPONENTS Foundation Net Util)

add_library(eglt-oss SHARED ${EGLT_PROJECT_ROOT}/library.cpp)
