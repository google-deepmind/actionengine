add_library(
        egltNodes STATIC
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
add_library(eglt::nodes ALIAS egltNodes)
target_include_directories(
        egltNodes PRIVATE
        ${EGLT_PROJECT_TOP_LEVEL}
        ${EGLT_INCLUDE_DIR}
        ${EGLT_THIRD_PARTY_INCLUDE_DIRS}
)
target_link_libraries(
        egltNodes PUBLIC
        cppitertools::cppitertools
        eglt::absl_headers
        eglt::concurrency
        eglt::data
)

install(
        TARGETS egltNodes
        EXPORT egltTargets
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib/eglt
        ARCHIVE DESTINATION lib/eglt
)