add_library(
        eglt_nodes STATIC
        async_node.cc
        async_node.h
        chunk_store.cc
        chunk_store.h
        chunk_store_io.cc
        chunk_store_io.h
        chunk_store_local.cc
        chunk_store_local.h
        node_map.cc
        node_map.h
)
add_library(eglt::nodes ALIAS eglt_nodes)
target_include_directories(
        eglt_nodes PRIVATE
        ${EGLT_PROJECT_TOP_LEVEL}
        ${EGLT_INCLUDE_DIRS}
)
target_link_libraries(
        eglt_nodes PUBLIC
        cppitertools::cppitertools
        eglt::absl_headers
        eglt::concurrency
        eglt::data
)