set(CMAKE_PREFIX_PATH
        ${CMAKE_PREFIX_PATH}
        ${G3_FIBER_ABSL_ROOT}
        ${G3_FIBER_GTEST_ROOT}
        ${G3_FIBER_BOOST_ROOT}
)

message(STATUS G3_FIBER_BOOST_ROOT: ${G3_FIBER_BOOST_ROOT})

find_package(GTest REQUIRED)
find_package(absl REQUIRED)

message(STATUS "Boost_INCLUDE_DIRS: ${Boost_INCLUDE_DIRS}")

add_library(G3Fiber STATIC)
target_include_directories(
        G3Fiber PUBLIC
        ${CMAKE_CURRENT_LIST_DIR}
        ${G3_FIBER_ABSL_ROOT}/include
        ${Boost_INCLUDE_DIRS}
        ${G3_FIBER_GTEST_ROOT}/include
)
target_sources(
        G3Fiber PRIVATE
        g3_fiber/absl_headers.h
        g3_fiber/channel.h
        g3_fiber/channel/waiter_state.cc
        g3_fiber/channel/waiter_state.h
        g3_fiber/fiber.cc
        g3_fiber/select.cc
        g3_fiber/cases.h
        g3_fiber/selectables.cc
        g3_fiber/thread_pool.cc
        g3_fiber/thread_pool.h
        g3_fiber/util.cc
)
target_sources(
        G3Fiber PUBLIC
        g3_fiber/boost_primitives.h
        g3_fiber/concurrency.h
        g3_fiber/fiber.h
        g3_fiber/select.h
        g3_fiber/selectables.h
        g3_fiber/util.h
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
message(${G3_FIBER_BOOST_ROOT}/../../boost/include)
set(G3_FIBER_INCLUDE_DIRS
        ${CMAKE_CURRENT_LIST_DIR}
        ${G3_FIBER_ABSL_ROOT}/include
        ${Boost_INCLUDE_DIRS}
        ${G3_FIBER_GTEST_ROOT}/include
        PARENT_SCOPE
)
