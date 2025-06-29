set(CMAKE_PREFIX_PATH
        ${CMAKE_PREFIX_PATH}
        ${THREAD_ON_BOOST_ABSL_ROOT}
        ${THREAD_ON_BOOST_GTEST_ROOT}
        ${THREAD_ON_BOOST_BOOST_ROOT}
)

message(STATUS THREAD_ON_BOOST_BOOST_ROOT: ${THREAD_ON_BOOST_BOOST_ROOT})

find_package(GTest REQUIRED)
find_package(absl REQUIRED)

message(STATUS "Boost_INCLUDE_DIRS: ${Boost_INCLUDE_DIRS}")

add_library(ThreadOnBoost STATIC)
target_include_directories(
        ThreadOnBoost PUBLIC
        ${CMAKE_CURRENT_LIST_DIR}
        ${THREAD_ON_BOOST_ABSL_ROOT}/include
        ${Boost_INCLUDE_DIRS}
        ${THREAD_ON_BOOST_GTEST_ROOT}/include
)
target_sources(
        ThreadOnBoost PRIVATE
        thread_on_boost/absl_headers.h
        thread_on_boost/channel.h
        thread_on_boost/channel/waiter_state.cc
        thread_on_boost/channel/waiter_state.h
        thread_on_boost/fiber.cc
        thread_on_boost/select.cc
        thread_on_boost/cases.h
        thread_on_boost/selectables.cc
        thread_on_boost/thread_pool.cc
        thread_on_boost/thread_pool.h
        thread_on_boost/util.cc
)
target_sources(
        ThreadOnBoost PUBLIC
        thread_on_boost/boost_primitives.h
        thread_on_boost/concurrency.h
        thread_on_boost/fiber.h
        thread_on_boost/select.h
        thread_on_boost/selectables.h
        thread_on_boost/util.h
)
target_link_libraries(
        ThreadOnBoost PRIVATE
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
        ThreadOnBoost PUBLIC
        boost_fiber
)
message(${THREAD_ON_BOOST_BOOST_ROOT}/../../boost/include)
set(THREAD_ON_BOOST_INCLUDE_DIRS
        ${CMAKE_CURRENT_LIST_DIR}
        ${THREAD_ON_BOOST_ABSL_ROOT}/include
        ${Boost_INCLUDE_DIRS}
        ${THREAD_ON_BOOST_GTEST_ROOT}/include
        PARENT_SCOPE
)
