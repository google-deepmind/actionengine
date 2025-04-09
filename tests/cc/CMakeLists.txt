add_executable(
        chunk_store_test
        chunk_store_test.cc
)
target_link_libraries(
        chunk_store_test
        eglt
        thread_on_boost
        GTest::gtest_main
)

include(GoogleTest)
gtest_discover_tests(chunk_store_test)