add_library(
        eglt_concurrency_boost_fiber INTERFACE
)
add_library(eglt::concurrency::boost_fiber ALIAS eglt_concurrency_boost_fiber)
target_precompile_headers(
        eglt_concurrency_boost_fiber INTERFACE
        boost_fiber.h
)
find_package(Boost REQUIRED COMPONENTS fiber)
message(STATUS "Boost_INCLUDE_DIRS: ${Boost_INCLUDE_DIRS}")
target_link_libraries(
        eglt_concurrency_boost_fiber INTERFACE
        eglt::absl_headers
        Boost::fiber
)

add_library(thread_on_boost STATIC)
target_include_directories(
        thread_on_boost PUBLIC
        ${CMAKE_CURRENT_LIST_DIR}
)
target_sources(
        thread_on_boost PRIVATE
        thread_on_boost/channel.cc
        thread_on_boost/channel.h
        thread_on_boost/channel-internal.h
        thread_on_boost/fiber.cc
        thread_on_boost/intrusive_list.h
        thread_on_boost/select.cc
        thread_on_boost/select-internal.cc
        thread_on_boost/select-internal.h
        thread_on_boost/selectables.cc
)
target_sources(
        thread_on_boost PUBLIC
        thread_on_boost/concurrency.h
        thread_on_boost/boost_primitives.h
        thread_on_boost/fiber.h
        thread_on_boost/select.h
        thread_on_boost/selectables.h
)
target_link_libraries(
        thread_on_boost PUBLIC
        Boost::fiber
        Boost::intrusive
        eglt::absl_headers
)
target_compile_definitions(
        thread_on_boost PUBLIC
)
target_compile_definitions(
        thread_on_boost INTERFACE
        __EGLT_CONCURRENCY_IMPLEMENTATION__
        __EGLT_CONCURRENCY_IMPLEMENTATION_THREAD_ON_BOOST__
)

add_library(
        eglt_concurrency INTERFACE
)
add_library(eglt::concurrency ALIAS eglt_concurrency)
target_precompile_headers(
        eglt_concurrency INTERFACE
        concurrency.h
)
target_compile_definitions(
        eglt_concurrency INTERFACE
        __EGLT_CONCURRENCY_IMPLEMENTATION__
        __EGLT_CONCURRENCY_IMPLEMENTATION_THREAD_ON_BOOST__
)
target_include_directories(
        eglt_concurrency INTERFACE
        ${EGLT_PROJECT_TOP_LEVEL}
        ${EGLT_INCLUDE_DIRS}
)
target_link_libraries(
        eglt_concurrency INTERFACE
        eglt::absl_headers
        thread_on_boost
)

