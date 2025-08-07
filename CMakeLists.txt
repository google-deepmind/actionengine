cmake_minimum_required(VERSION 3.22)

if (APPLE)
    set(CMAKE_OSX_DEPLOYMENT_TARGET "15.0" CACHE STRING "Minimum OS X deployment version")
endif ()

project(actionengine
        VERSION 0.1
        LANGUAGES C CXX
)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wthread-safety -Werror=thread-safety")
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

set(EGLT_PROJECT_TOP_LEVEL ${CMAKE_CURRENT_SOURCE_DIR}/src)
set(EGLT_PROJECT_ROOT ${EGLT_PROJECT_TOP_LEVEL}/eglt)
set(EGLT_PYBIND11_TOP_LEVEL ${CMAKE_CURRENT_SOURCE_DIR}/pybind11)

set(EGLT_THIRD_PARTY_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/third_party)
set(EGLT_THIRD_PARTY_INSTALL_ROOT ${EGLT_THIRD_PARTY_ROOT}/build_deps)

set(EGLT_ABSL_ROOT ${EGLT_THIRD_PARTY_INSTALL_ROOT}/abseil-cpp)
set(EGLT_PYBIND11_ROOT ${EGLT_THIRD_PARTY_INSTALL_ROOT}/pybind11)
set(EGLT_CPPITERTOOLS_ROOT ${EGLT_THIRD_PARTY_INSTALL_ROOT}/cppitertools)
set(EGLT_GTEST_ROOT ${EGLT_THIRD_PARTY_INSTALL_ROOT}/googletest)
set(EGLT_LIBDATACHANNEL_ROOT ${EGLT_THIRD_PARTY_INSTALL_ROOT}/libdatachannel)
set(EGLT_HIREDIS_ROOT ${EGLT_THIRD_PARTY_INSTALL_ROOT}/hiredis)
set(EGLT_LIBUV_ROOT ${EGLT_THIRD_PARTY_INSTALL_ROOT}/libuv)
set(EGLT_UVW_ROOT ${EGLT_THIRD_PARTY_INSTALL_ROOT}/uvw)

add_compile_definitions(
        BOOST_FIBERS_SPINLOCK_TTAS_FUTEX
)
add_subdirectory(${EGLT_THIRD_PARTY_ROOT}/boost)
set(
        Boost_INCLUDE_DIRS
        ""
)

set(G3FIBER_BOOST_ROOT ${EGLT_THIRD_PARTY_INSTALL_ROOT}/boost)
set(G3FIBER_ABSL_ROOT ${EGLT_ABSL_ROOT})
set(G3FIBER_GTEST_ROOT ${EGLT_GTEST_ROOT})
set(G3FIBER_DIR ${CMAKE_CURRENT_LIST_DIR}/src/g3fiber)
add_subdirectory(${G3FIBER_DIR})

set(EGLT_INCLUDE_DIR ${CMAKE_CURRENT_LIST_DIR}/include)

set(CMAKE_PREFIX_PATH
        ${CMAKE_PREFIX_PATH}
        #        ${EGLT_ABSL_ROOT}
        ${EGLT_PYBIND11_ROOT}
        ${EGLT_CPPITERTOOLS_ROOT}
        ${EGLT_GTEST_ROOT}
        ${EGLT_LIBDATACHANNEL_ROOT}
        ${EGLT_HIREDIS_ROOT}
        ${EGLT_LIBUV_ROOT}
        ${EGLT_UVW_ROOT}
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
find_package(GTest REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(LibDataChannel REQUIRED)
find_package(hiredis REQUIRED)
find_package(libuv REQUIRED)
find_package(uvw REQUIRED)

add_compile_definitions(UVW_AS_LIB)

add_subdirectory(${CMAKE_CURRENT_LIST_DIR}/src/cppack)

include(FetchContent)
set(BUILD_TESTING_BACKUP ${BUILD_TESTING})
set(USE_SYSTEM_ABSEIL_BACKUP ${USE_SYSTEM_ABSEIL})
set(USE_SYSTEM_PYBIND_BACKUP ${USE_SYSTEM_PYBIND})
set(BUILD_TESTING OFF)
set(USE_SYSTEM_ABSEIL ON)
set(USE_SYSTEM_PYBIND ON)
add_subdirectory(${EGLT_THIRD_PARTY_ROOT}/pybind11_abseil)
set(BUILD_TESTING ${BUILD_TESTING_BACKUP})
set(USE_SYSTEM_ABSEIL ${USE_SYSTEM_ABSEIL_BACKUP})
set(USE_SYSTEM_PYBIND ${USE_SYSTEM_PYBIND_BACKUP})

set(EGLT_THIRD_PARTY_INCLUDE_DIRS
        ${EGLT_ABSL_ROOT}/include
        ${G3FIBER_DIR}
        ${EGLT_PYBIND11_ROOT}/include
        ${EGLT_CPPITERTOOLS_ROOT}/include
        ${EGLT_GTEST_ROOT}/include
        ${EGLT_LIBDATACHANNEL_ROOT}/include
        ${EGLT_HIREDIS_ROOT}/include
        ${EGLT_LIBUV_ROOT}/include
        ${EGLT_UVW_ROOT}/include
)

enable_testing()
include(GoogleTest)
add_subdirectory(src)
add_subdirectory(examples)

install(
        TARGETS
        G3Fiber
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
)