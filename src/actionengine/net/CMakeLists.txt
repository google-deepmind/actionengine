add_library(
        actNet STATIC
        client.cc
        testing_utils.h
        stream.h
        recoverable_stream.h
)
add_library(act::net ALIAS actNet)
target_include_directories(
        actNet PUBLIC
        ${ACTIONENGINE_PROJECT_TOP_LEVEL}
        ${ACTIONENGINE_INCLUDE_DIR}
)
target_link_libraries(
        actNet PUBLIC
        act::concurrency
        act::data
)

# -----------------------------------------------------------------------------

add_library(
        actNetTesting INTERFACE
        testing_utils.h
)
add_library(act::net::testing ALIAS actNetTesting)
target_include_directories(
        actNet INTERFACE
        ${ACTIONENGINE_PROJECT_TOP_LEVEL}
        ${ACTIONENGINE_INCLUDE_DIR}
)
target_link_libraries(
        actNet INTERFACE
        act::concurrency
        act::net
        act::data
)

# -----------------------------------------------------------------------------

add_executable(
        test_recoverable_stream
        test_recoverable_stream.cc
        ${ACTIONENGINE_EXPERIMENTAL_HEADERS}
)
target_link_libraries(
        test_recoverable_stream
        act::net
        act::net::testing
        GTest::gtest
        GTest::gtest_main
)

gtest_discover_tests(test_recoverable_stream)

# -----------------------------------------------------------------------------

add_executable(
        test_net_utils
        test_utils.cc
)
target_link_libraries(
        test_net_utils
        act::net
        act::net::testing
        GTest::gtest
        GTest::gtest_main
)

gtest_discover_tests(test_net_utils)

# -----------------------------------------------------------------------------

install(
        TARGETS actNet
        EXPORT actTargets
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib/act
        ARCHIVE DESTINATION lib/act
)

add_subdirectory(webrtc)
add_subdirectory(websockets)