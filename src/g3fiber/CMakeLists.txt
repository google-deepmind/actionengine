set(CMAKE_PREFIX_PATH
        ${CMAKE_PREFIX_PATH}
        ${G3FIBER_ABSL_ROOT}
        ${G3FIBER_GTEST_ROOT}
        ${G3FIBER_BOOST_ROOT}
)

message(STATUS G3FIBER_BOOST_ROOT: ${G3FIBER_BOOST_ROOT})

find_package(GTest REQUIRED)
find_package(absl REQUIRED)

message(STATUS "Boost_INCLUDE_DIRS: ${Boost_INCLUDE_DIRS}")

add_library(G3Fiber STATIC)
target_include_directories(
        G3Fiber PUBLIC
        ${CMAKE_CURRENT_LIST_DIR}
        ${G3FIBER_ABSL_ROOT}/include
        ${Boost_INCLUDE_DIRS}
        ${G3FIBER_GTEST_ROOT}/include
)
target_sources(
        G3Fiber PRIVATE
        g3fiber/absl_headers.h
        g3fiber/channel.h
        g3fiber/channel/waiter_state.cc
        g3fiber/channel/waiter_state.h
        g3fiber/fiber.cc
        g3fiber/select.cc
        g3fiber/cases.h
        g3fiber/selectables.cc
        g3fiber/thread_pool.cc
        g3fiber/thread_pool.h
        g3fiber/util.cc
)
target_sources(
        G3Fiber PUBLIC
        g3fiber/boost_primitives.h
        g3fiber/concurrency.h
        g3fiber/fiber.h
        g3fiber/select.h
        g3fiber/selectables.h
        g3fiber/util.h
)
target_link_libraries(
        G3Fiber PRIVATE
        absl::base
        absl::flags
        absl::flat_hash_map
        absl::log
        absl::log_initialize
        absl::log_internal_check_op
        absl::random_random
        absl::time
        boost_thread
)
target_link_libraries(
        G3Fiber PUBLIC
        boost_fiber
)
message(${G3FIBER_BOOST_ROOT}/../../boost/include)
set(G3FIBER_INCLUDE_DIRS
        ${CMAKE_CURRENT_LIST_DIR}
        ${G3FIBER_ABSL_ROOT}/include
        ${Boost_INCLUDE_DIRS}
        ${G3FIBER_GTEST_ROOT}/include
        PARENT_SCOPE
)
