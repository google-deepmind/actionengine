set(CMAKE_PREFIX_PATH
        ${CMAKE_PREFIX_PATH}
        ${THREAD_ABSL_ROOT}
        ${THREAD_GTEST_ROOT}
        ${THREAD_BOOST_ROOT}
)

message(STATUS THREAD_BOOST_ROOT: ${THREAD_BOOST_ROOT})

find_package(GTest REQUIRED)
find_package(absl REQUIRED)

message(STATUS "Boost_INCLUDE_DIRS: ${Boost_INCLUDE_DIRS}")

add_library(Thread STATIC)
target_include_directories(
        Thread PUBLIC
        ${CMAKE_CURRENT_LIST_DIR}
        ${THREAD_GTEST_ROOT}/include
)

target_include_directories(
        Thread PRIVATE
        ${Boost_INCLUDE_DIRS}
        ${THREAD_ABSL_ROOT}/include
)
target_sources(
        Thread PRIVATE
        thread/boost_primitives.cc
        thread/channel/waiter_state.cc
        thread/channel/waiter_state.h
        thread/fiber.cc
        thread/select.cc
        thread/selectables.cc
        thread/thread_pool.cc
        thread/util.cc
)
target_sources(
        Thread PUBLIC
        thread/cases.h
        thread/boost_primitives.h
        thread/channel.h
        thread/concurrency.h
        thread/fiber.h
        thread/select.h
        thread/selectables.h
        thread/thread_pool.h
        thread/util.h
)
target_link_libraries(
        Thread PRIVATE
        absl::base
        absl::flags
        absl::flat_hash_map
        absl::log
        absl::log_initialize
        absl::log_internal_check_op
        absl::random_random
        absl::time
        Boost::thread
)
target_include_directories(
        Thread PUBLIC
        ${ACTIONENGINE_THIRD_PARTY_ROOT}/boost/libs/intrusive/include
        ${ACTIONENGINE_THIRD_PARTY_ROOT}/boost/libs/fiber/include
        ${ACTIONENGINE_THIRD_PARTY_ROOT}/boost/libs/smart_ptr/include
)
target_link_libraries(
        Thread PUBLIC
        Boost::fiber
)
target_compile_options(
        Thread PRIVATE
        -Wall -Wextra -Wno-nullability-completeness -Wno-sign-compare -Wthread-safety -Werror=thread-safety
)
message(${THREAD_BOOST_ROOT}/../../boost/include)
set(THREAD_INCLUDE_DIRS
        ${CMAKE_CURRENT_LIST_DIR}
        ${THREAD_ABSL_ROOT}/include
        ${THREAD_GTEST_ROOT}/include
        PARENT_SCOPE
)
