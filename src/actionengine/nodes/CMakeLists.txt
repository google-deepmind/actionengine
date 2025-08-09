add_library(
        actNodes STATIC
        async_node.cc
        async_node.h
        node_map.cc
        node_map.h
)
add_library(act::nodes ALIAS actNodes)
target_include_directories(
        actNodes PUBLIC
        ${ACTIONENGINE_PROJECT_TOP_LEVEL}
        ${ACTIONENGINE_INCLUDE_DIR}
        ${ACTIONENGINE_THIRD_PARTY_INCLUDE_DIRS}
)
target_link_libraries(
        actNodes PRIVATE
        absl::base
        absl::flat_hash_map
)
target_link_libraries(
        actNodes PUBLIC
        cppitertools::cppitertools
        act::concurrency
        act::data
        act::stores
)

# ------------------------------------------------------------------

add_library(
        actNodesPybind11 STATIC
)
add_library(
        act::nodes::pybind11 ALIAS
        actNodesPybind11
)
target_sources(
        actNodesPybind11 PRIVATE
        nodes_pybind11.cc
)
target_sources(
        actNodesPybind11 PUBLIC
        nodes_pybind11.h
)
target_link_libraries(
        actNodesPybind11 PRIVATE
        act::nodes
        ${ACTIONENGINE_PYBIND11_DEPENDENCIES}
)
target_include_directories(
        actNodesPybind11 PUBLIC
        ${ACTIONENGINE_PYBIND11_INCLUDE_DIRS}
)

install(
        TARGETS actNodes
        EXPORT actTargets
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib/act
        ARCHIVE DESTINATION lib/act
)