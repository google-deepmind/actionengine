add_library(
        actStores STATIC
        byte_chunking.cc
        byte_chunking.h
        chunk_store.h
        chunk_store.cc
        chunk_store_reader.h
        chunk_store_reader.cc
        chunk_store_writer.h
        chunk_store_writer.cc
        local_chunk_store.cc
        local_chunk_store.h
)
add_library(act::stores ALIAS actStores)
target_include_directories(
        actStores PUBLIC
        ${ACTIONENGINE_PROJECT_TOP_LEVEL}
        ${ACTIONENGINE_INCLUDE_DIR}
        ${ACTIONENGINE_THIRD_PARTY_INCLUDE_DIRS}
)
target_link_libraries(
        actStores PRIVATE
        absl::log
        absl::log_internal_check_op
        absl::time
        cppack
        act::data
)
target_link_libraries(
        actStores PUBLIC
        cppitertools::cppitertools
        act::concurrency
        act::data
        act::net
        act::util
)

# ------------------------------------------------------------------

add_library(
        actStoresPybind11 STATIC
)
add_library(
        act::stores::pybind11 ALIAS
        actStoresPybind11
)
target_sources(
        actStoresPybind11 PRIVATE
        chunk_store_pybind11.cc
)
target_sources(
        actStoresPybind11 PUBLIC
        chunk_store_pybind11.h
)
target_link_libraries(
        actStoresPybind11 PRIVATE
        act::stores
        ${ACTIONENGINE_PYBIND11_DEPENDENCIES}
)
target_include_directories(
        actStoresPybind11 PUBLIC
        ${ACTIONENGINE_PYBIND11_INCLUDE_DIRS}
)

# -----------------------------------------------------------------------------

add_executable(
        test_chunk_store
        test_chunk_store.cc
)
target_link_libraries(
        test_chunk_store
        absl::status_matchers
        act::data
        act::stores
        GTest::gtest
        GTest::gtest_main
)

gtest_discover_tests(test_chunk_store)

install(
        TARGETS actStoresPybind11
        EXPORT actTargets
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib/act
        ARCHIVE DESTINATION lib/act
)