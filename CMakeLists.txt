cmake_minimum_required(VERSION 3.30)
project(eglt-oss
        VERSION 0.1
        LANGUAGES CXX
)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wthread-safety -Werror=thread-safety")

set(EGLT_PROJECT_TOP_LEVEL ${CMAKE_CURRENT_SOURCE_DIR}/eglt)
set(EGLT_PROJECT_ROOT ${EGLT_PROJECT_TOP_LEVEL}/eglt)

set(EGLT_THIRD_PARTY_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/third_party)
set(EGLT_THIRD_PARTY_INSTALL_ROOT ${EGLT_THIRD_PARTY_ROOT}/build_deps)

set(EGLT_INCLUDE_DIR ${CMAKE_CURRENT_LIST_DIR}/include)

set(CMAKE_PREFIX_PATH
    ${CMAKE_PREFIX_PATH}
    ${EGLT_THIRD_PARTY_INSTALL_ROOT}/abseil-cpp
    ${EGLT_THIRD_PARTY_INSTALL_ROOT}/pybind11
    ${EGLT_THIRD_PARTY_ROOT}/pybind11_abseil
    ${EGLT_THIRD_PARTY_INSTALL_ROOT}/boost
    ${EGLT_THIRD_PARTY_INSTALL_ROOT}/cppitertools
    ${EGLT_THIRD_PARTY_INSTALL_ROOT}/fiber
    ${EGLT_THIRD_PARTY_INSTALL_ROOT}/googletest
    ${EGLT_THIRD_PARTY_INSTALL_ROOT}/poco
)

execute_process(
    COMMAND python3 -c "import sys; print(sys.executable, end='')"
    OUTPUT_VARIABLE Python_EXECUTABLE
)
execute_process(
    COMMAND python3 -c "import sys; print(sys.executable, end='')"
    OUTPUT_VARIABLE Python3_EXECUTABLE
)
find_package(Python REQUIRED COMPONENTS Development Interpreter)
find_package(Python3 REQUIRED COMPONENTS Development.Module Interpreter)
set(pybind11_DIR ${EGLT_THIRD_PARTY_INSTALL_ROOT}/pybind11/lib/cmake/pybind11)
find_package(pybind11 REQUIRED)

find_package(GTest REQUIRED)
find_package(absl REQUIRED)
find_package(cppitertools REQUIRED)
find_package(Boost REQUIRED COMPONENTS fiber)
find_package(GTest REQUIRED)
find_package(Poco REQUIRED COMPONENTS Foundation Net Util)

add_subdirectory(${EGLT_THIRD_PARTY_ROOT}/pybind11_abseil)

set(EGLT_INCLUDE_DIRS
    ${EGLT_INCLUDE_DIR}
    ${EGLT_THIRD_PARTY_INSTALL_ROOT}/abseil-cpp/include
    ${EGLT_THIRD_PARTY_INSTALL_ROOT}/pybind11/include
    ${EGLT_THIRD_PARTY_ROOT}/pybind11_abseil
    ${EGLT_THIRD_PARTY_INSTALL_ROOT}/boost/include
    ${EGLT_THIRD_PARTY_INSTALL_ROOT}/cppitertools/include
    ${EGLT_THIRD_PARTY_INSTALL_ROOT}/googletest/include
    ${EGLT_THIRD_PARTY_INSTALL_ROOT}/poco/include
)


add_subdirectory(eglt)

add_library(eglt-oss SHARED ${EGLT_PROJECT_ROOT}/library.cpp)
target_link_libraries(eglt-oss eglt_absl_headers eglt_actions eglt_data eglt_concurrency eglt_nodes eglt_net eglt_util)
target_include_directories(
    eglt-oss
    PUBLIC
    ${EGLT_INCLUDE_DIRS}
)
