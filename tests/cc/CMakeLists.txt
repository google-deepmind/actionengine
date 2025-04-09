add_executable(
        test_chunk_store
        test_chunk_store.cc
)
target_link_libraries(
        test_chunk_store
        eglt::core
        GTest::gtest
        GTest::gtest_main
)

include(GoogleTest)
gtest_discover_tests(test_chunk_store)