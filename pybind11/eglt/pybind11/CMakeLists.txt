pybind11_add_module(
        evergreen
        evergreen.cc
)

target_precompile_headers(
        evergreen PRIVATE
        pybind11_headers.h
)

target_sources(
        evergreen PRIVATE
        actions.cc
        actions.h
        chunk_store.cc
        chunk_store.h
        nodes.cc
        nodes.h
        service.cc
        service.h
        types.cc
        types.h
        utils.cc
        utils.h
)

target_link_libraries(
        evergreen PRIVATE
        eglt_absl_headers
        eglt_actions
        eglt_data
        eglt_concurrency
        eglt_net
        eglt_nodes
        eglt_service
        eglt_util
)

target_include_directories(
        evergreen PRIVATE
        ${EGLT_INCLUDE_DIR}
        ${EGLT_INCLUDE_DIRS}
        ${EGLT_THIRD_PARTY_INSTALL_ROOT}/pybind11_abseil
        ${EGLT_PROJECT_TOP_LEVEL}
        ${EGLT_PYBIND11_TOP_LEVEL}
)