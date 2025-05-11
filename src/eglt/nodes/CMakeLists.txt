add_library(
        egltNodes STATIC
        async_node.cc
        async_node.h
        chunk_store.h
        chunk_store_io.h
        chunk_store_reader.h
        chunk_store_writer.h
        local_chunk_store.h
        node_map.cc
        node_map.h
        shared_chunk_store.h
)
add_library(eglt::nodes ALIAS egltNodes)
target_include_directories(
        egltNodes PUBLIC
        ${EGLT_PROJECT_TOP_LEVEL}
        ${EGLT_INCLUDE_DIR}
        ${EGLT_THIRD_PARTY_INCLUDE_DIRS}
)
target_link_libraries(
        egltNodes PUBLIC
        cppitertools::cppitertools
        ${EGLT_ABSL_DEPENDENCIES}
        eglt::concurrency
        eglt::data
)

# ------------------------------------------------------------------

add_library(
        egltNodesPybind11 STATIC
)
add_library(
        eglt::nodes::pybind11 ALIAS
        egltNodesPybind11
)
target_sources(
        egltNodesPybind11 PRIVATE
        chunk_store_pybind11.cc
        nodes_pybind11.cc
)
target_sources(
        egltNodesPybind11 PUBLIC
        chunk_store_pybind11.h
        nodes_pybind11.h
)
target_link_libraries(
        egltNodesPybind11 PRIVATE
        eglt::nodes
        ${EGLT_PYBIND11_DEPENDENCIES}
)
target_include_directories(
        egltNodesPybind11 PUBLIC
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
        eglt::nodes
        GTest::gtest
        GTest::gtest_main
)

gtest_discover_tests(test_chunk_store)

install(
        TARGETS egltNodes
        EXPORT egltTargets
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib/eglt
        ARCHIVE DESTINATION lib/eglt
)