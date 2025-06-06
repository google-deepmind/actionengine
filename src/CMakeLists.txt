set(
        EGLT_ABSL_DEPENDENCIES
        absl::base
        absl::debugging
        absl::debugging_internal
        absl::failure_signal_handler
        absl::flags
        absl::flags_parse
        absl::flat_hash_map
        absl::log
        absl::log_initialize
        absl::log_internal_check_op
        absl::random_random
        absl::stacktrace
        absl::status
        absl::status_matchers
        absl::statusor
        absl::strings
        absl::strings_internal
        absl::symbolize
        absl::time
)
set(EGLT_ABSL_DEPENDENCIES ${EGLT_ABSL_DEPENDENCIES} PARENT_SCOPE)

# ------------------------------------------------------------------

set(EGLT_PYBIND11_DEPENDENCIES
        pybind11::module
        pybind11::embed
        pybind11_abseil::status_casters
        pybind11_abseil::absl_casters
        pybind11_abseil::import_status_module
        pybind11_abseil::statusor_caster
)
set(EGLT_PYBIND11_DEPENDENCIES ${EGLT_PYBIND11_DEPENDENCIES} PARENT_SCOPE)

set(
        EGLT_PYBIND11_INCLUDE_DIRS
        ${EGLT_THIRD_PARTY_INSTALL_ROOT}/pybind11_abseil
        ${Python_INCLUDE_DIRS}
        ${EGLT_PYBIND11_TOP_LEVEL}
)
set(EGLT_PYBIND11_INCLUDE_DIRS ${EGLT_PYBIND11_INCLUDE_DIRS} PARENT_SCOPE)

# ------------------------------------------------------------------

add_library(
        eglt_testing INTERFACE
)
add_library(
        eglt::testing ALIAS eglt_testing
)
set_target_properties(eglt_testing PROPERTIES
        LANGUAGE CXX
        LINKER_LANGUAGE CXX
)
target_include_directories(
        eglt_testing
        INTERFACE
        ${EGLT_INCLUDE_DIR}
        ${EGLT_THIRD_PARTY_INSTALL_ROOT}/googletest/include
)

add_subdirectory(eglt/actions)
add_subdirectory(eglt/concurrency)
add_subdirectory(eglt/data)
add_subdirectory(eglt/net)
add_subdirectory(eglt/nodes)
add_subdirectory(eglt/service)
add_subdirectory(eglt/stores)
add_subdirectory(eglt/util)

# ------------------------------------------------------------------

pybind11_add_module(
        evergreen_pybind11
        eglt/evergreen_pybind11.cc
)
add_library(
        eglt::pybind11 ALIAS
        evergreen_pybind11
)

target_link_libraries(
        evergreen_pybind11 PRIVATE
        eglt::data
        eglt::data::pybind11
        eglt::actions
        eglt::actions::pybind11
        eglt::concurrency
        eglt::net
        eglt::service
        eglt::service::pybind11
        eglt::stores
        eglt::stores::pybind11
        eglt::nodes
        eglt::nodes::pybind11
        ${EGLT_ABSL_DEPENDENCIES}
        eglt::sdk::webrtc
        eglt::sdk::webrtc::pybind11
        eglt::sdk::websockets
        eglt::sdk::websockets::pybind11
        eglt::util::pybind11
        ${EGLT_PYBIND11_DEPENDENCIES}
)

target_include_directories(
        evergreen_pybind11 PRIVATE
        ${EGLT_INCLUDE_DIR}
        ${EGLT_THIRD_PARTY_INCLUDE_DIRS}
        ${EGLT_THIRD_PARTY_INSTALL_ROOT}/pybind11_abseil
        ${EGLT_PROJECT_TOP_LEVEL}
        ${EGLT_PYBIND11_INCLUDE_DIRS}
        ${EGLT_PYBIND11_TOP_LEVEL}
)

install(
        TARGETS evergreen_pybind11
        EXPORT egltTargets
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib/eglt
        ARCHIVE DESTINATION lib/eglt
)