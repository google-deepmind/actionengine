pybind11_add_module(
        evergreen_pybind11
        evergreen.cc
)
add_library(
        eglt::pybind11 ALIAS
        evergreen_pybind11
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
        pybind11_headers.h
        types.cc
        types.h
        utils.cc
        utils.h
        websockets.cc
        websockets.h
)

target_link_libraries(
        evergreen_pybind11 PRIVATE
        eglt::data
        eglt::actions
        eglt::concurrency
        eglt::net
        eglt::service
        eglt::nodes
        eglt::absl_headers
        eglt::sdk::serving::websockets
        eglt::util
        register_status_bindings
        pybind11_abseil::status_casters
        pybind11_abseil::absl_casters
        pybind11_abseil::import_status_module
        pybind11_abseil::statusor_caster
)

target_include_directories(
        evergreen_pybind11 PRIVATE
        ${EGLT_INCLUDE_DIR}
        ${EGLT_THIRD_PARTY_INCLUDE_DIRS}
        ${EGLT_THIRD_PARTY_INSTALL_ROOT}/pybind11_abseil
        ${EGLT_PROJECT_TOP_LEVEL}
        ${EGLT_PYBIND11_TOP_LEVEL}
)

install(
        TARGETS evergreen_pybind11
        EXPORT egltTargets
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib/eglt
        ARCHIVE DESTINATION lib/eglt
)