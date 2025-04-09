pybind11_add_module(
        evergreen_pybind11
        evergreen.cc
)
add_library(
        eglt::pybind11 ALIAS
        evergreen_pybind11
)

target_precompile_headers(
        evergreen_pybind11 PRIVATE
        pybind11_headers.h
)

target_sources(
        evergreen_pybind11 PRIVATE
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
        evergreen_pybind11 PRIVATE
        eglt::core
        pybind11_abseil::status_casters
)

target_include_directories(
        evergreen_pybind11 PRIVATE
        ${EGLT_INCLUDE_DIR}
        ${EGLT_THIRD_PARTY_INCLUDE_DIRS}
        ${EGLT_THIRD_PARTY_INSTALL_ROOT}/pybind11_abseil
        ${EGLT_PROJECT_TOP_LEVEL}
        ${EGLT_PYBIND11_TOP_LEVEL}
)