find_package(ZLIB REQUIRED)

add_library(actDistributed STATIC
        consistent_hash.h
        consistent_hash.cc
        groupcache.cc
        groupcache.h
        lru.cc
        lru.h
        peers.cc
        peers.h
        singleflight.cc
        singleflight.h
        sinks.cc
        sinks.h
)
add_library(act::distributed ALIAS actDistributed)
target_include_directories(
        actDistributed PRIVATE
        ${ACTIONENGINE_PROJECT_TOP_LEVEL}
        ${ACTIONENGINE_INCLUDE_DIR}
        ${ACTIONENGINE_THIRD_PARTY_INCLUDE_DIRS}
)
target_link_libraries(
        actDistributed PRIVATE
        cppack
        absl::crc32c
        hiredis::hiredis
        act::concurrency
        act::data
        act::msgpack
        act::stores
        act::util
        Thread
        libuv::uv_a
        uvw::uvw
        ZLIB::ZLIB
)

# -----------------------------------------------------------------------------

add_executable(
        test_consistent_hash
        test_consistent_hash.cc
)
target_link_libraries(
        test_consistent_hash
        absl::status_matchers
        act::data
        act::distributed
        act::stores
        GTest::gtest
        GTest::gtest_main
)

gtest_discover_tests(test_consistent_hash)

# -----------------------------------------------------------------------------

add_executable(
        test_lru
        test_lru.cc
)
target_link_libraries(
        test_lru
        absl::status_matchers
        act::data
        act::distributed
        act::stores
        GTest::gtest
        GTest::gtest_main
)

gtest_discover_tests(test_lru)

# -----------------------------------------------------------------------------

add_executable(
        test_singleflight
        test_singleflight.cc
)
target_link_libraries(
        test_singleflight
        absl::status_matchers
        act::data
        act::distributed
        act::stores
        GTest::gtest
        GTest::gtest_main
)

gtest_discover_tests(test_singleflight)

# -----------------------------------------------------------------------------

add_executable(
        test_groupcache
        test_groupcache.cc
)
target_link_libraries(
        test_groupcache
        absl::status_matchers
        act::data
        act::distributed
        act::stores
        GTest::gtest
        GTest::gtest_main
)

gtest_discover_tests(test_singleflight)