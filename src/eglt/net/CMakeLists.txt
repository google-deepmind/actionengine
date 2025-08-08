add_library(
        egltNet STATIC
        client.cc
        testing_utils.h
        stream.h
        recoverable_stream.h
)
add_library(eglt::net ALIAS egltNet)
target_include_directories(
        egltNet PUBLIC
        ${EGLT_PROJECT_TOP_LEVEL}
        ${EGLT_INCLUDE_DIR}
)
target_link_libraries(
        egltNet PUBLIC
        eglt::concurrency
        eglt::data
)

# -----------------------------------------------------------------------------

add_library(
        egltNetTesting INTERFACE
        testing_utils.h
)
add_library(eglt::net::testing ALIAS egltNetTesting)
target_include_directories(
        egltNet INTERFACE
        ${EGLT_PROJECT_TOP_LEVEL}
        ${EGLT_INCLUDE_DIR}
)
target_link_libraries(
        egltNet INTERFACE
        eglt::concurrency
        eglt::net
        eglt::data
)

# -----------------------------------------------------------------------------

add_executable(
        test_recoverable_stream
        test_recoverable_stream.cc
        ${EGLT_EXPERIMENTAL_HEADERS}
)
target_link_libraries(
        test_recoverable_stream
        eglt::net
        eglt::net::testing
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
        eglt::net
        eglt::net::testing
        GTest::gtest
        GTest::gtest_main
)

gtest_discover_tests(test_net_utils)

# -----------------------------------------------------------------------------

install(
        TARGETS egltNet
        EXPORT egltTargets
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib/eglt
        ARCHIVE DESTINATION lib/eglt
)

add_subdirectory(webrtc)
add_subdirectory(websockets)