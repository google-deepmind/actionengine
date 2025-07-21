add_library(
        egltStores STATIC
        byte_chunking.cc
        byte_chunking.h
        chunk_store.h
        chunk_store.cc
        chunk_store_io.h
        chunk_store_reader.h
        chunk_store_writer.h
        local_chunk_store.h
)
add_library(eglt::stores ALIAS egltStores)
target_include_directories(
        egltStores PUBLIC
        ${EGLT_PROJECT_TOP_LEVEL}
        ${EGLT_INCLUDE_DIR}
        ${EGLT_THIRD_PARTY_INCLUDE_DIRS}
)
target_link_libraries(
        egltStores PUBLIC
        cppitertools::cppitertools
        ${EGLT_ABSL_DEPENDENCIES}
        eglt::concurrency
        eglt::data
        eglt::net
)

# ------------------------------------------------------------------

add_library(
        egltStoresPybind11 STATIC
)
add_library(
        eglt::stores::pybind11 ALIAS
        egltStoresPybind11
)
target_sources(
        egltStoresPybind11 PRIVATE
        chunk_store_pybind11.cc
)
target_sources(
        egltStoresPybind11 PUBLIC
        chunk_store_pybind11.h
)
target_link_libraries(
        egltStoresPybind11 PRIVATE
        eglt::stores
        ${EGLT_PYBIND11_DEPENDENCIES}
)
target_include_directories(
        egltStoresPybind11 PUBLIC
        ${EGLT_PYBIND11_INCLUDE_DIRS}
)

# -----------------------------------------------------------------------------

add_executable(
        test_chunk_store
        test_chunk_store.cc
)
target_link_libraries(
        test_chunk_store
        eglt::data
        eglt::stores
        GTest::gtest
        GTest::gtest_main
)

gtest_discover_tests(test_chunk_store)

install(
        TARGETS egltStoresPybind11
        EXPORT egltTargets
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib/eglt
        ARCHIVE DESTINATION lib/eglt
)