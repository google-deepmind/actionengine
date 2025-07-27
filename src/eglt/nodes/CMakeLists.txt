add_library(
        egltNodes STATIC
        async_node.cc
        async_node.h
        node_map.cc
        node_map.h
)
add_library(eglt::nodes ALIAS egltNodes)
target_include_directories(
        egltNodes PUBLIC
        ${EGLT_PROJECT_TOP_LEVEL}
        ${EGLT_INCLUDE_DIR}
        ${EGLT_THIRD_PARTY_INCLUDE_DIRS}
)
target_link_libraries(
        egltNodes PRIVATE
        absl::base
        absl::flat_hash_map
)
target_link_libraries(
        egltNodes PUBLIC
        cppitertools::cppitertools
        eglt::concurrency
        eglt::data
        eglt::stores
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
        nodes_pybind11.cc
)
target_sources(
        egltNodesPybind11 PUBLIC
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

install(
        TARGETS egltNodes
        EXPORT egltTargets
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib/eglt
        ARCHIVE DESTINATION lib/eglt
)