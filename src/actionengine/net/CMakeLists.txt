add_library(
        actNet STATIC
        client.cc
        stream.h
        recoverable_stream.cc
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

add_subdirectory(webrtc)
add_subdirectory(websockets)

# -----------------------------------------------------------------------------

add_executable(
        test_recoverable_stream
        test_recoverable_stream.cc
        ${ACTIONENGINE_EXPERIMENTAL_HEADERS}
)
target_link_libraries(
        test_recoverable_stream
        act::net
        act::net::websockets
        GTest::gtest
        GTest::gtest_main
)

gtest_discover_tests(test_recoverable_stream)

# -----------------------------------------------------------------------------

install(
        TARGETS actNet
        EXPORT actTargets
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib/act
        ARCHIVE DESTINATION lib/act
)