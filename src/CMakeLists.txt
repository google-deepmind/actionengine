SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-nullability-completeness")

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
add_subdirectory(eglt/msgpack)
add_subdirectory(eglt/net)
add_subdirectory(eglt/nodes)
add_subdirectory(eglt/redis)
add_subdirectory(eglt/service)
add_subdirectory(eglt/stores)
add_subdirectory(eglt/util)
add_subdirectory(pybind11_abseil_module)

# ------------------------------------------------------------------

pybind11_add_module(
        actionengine_pybind11
        eglt/actionengine_pybind11.cc
)
add_library(
        eglt::pybind11 ALIAS
        actionengine_pybind11
)

target_link_libraries(
        actionengine_pybind11 PRIVATE
        absl::failure_signal_handler
)

target_link_libraries(
        actionengine_pybind11 PRIVATE
        absl::base
        absl::flags
        absl::flags_parse
        absl::log
        absl::log_internal_check_op
        absl::status
        absl::statusor
        absl::strings
        absl::strings_internal
        absl::failure_signal_handler
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
        eglt::net::webrtc
        eglt::net::webrtc::pybind11
        eglt::net::websockets
        eglt::net::websockets::pybind11
        eglt::redis
        eglt::redis::pybind11
        eglt::util::pybind11
        ${EGLT_PYBIND11_DEPENDENCIES}
)

target_include_directories(
        actionengine_pybind11 PRIVATE
        ${EGLT_INCLUDE_DIR}
        ${EGLT_THIRD_PARTY_INCLUDE_DIRS}
        ${EGLT_THIRD_PARTY_INSTALL_ROOT}/pybind11_abseil
        ${EGLT_PROJECT_TOP_LEVEL}
        ${EGLT_PYBIND11_INCLUDE_DIRS}
        ${EGLT_PYBIND11_TOP_LEVEL}
)

install(
        TARGETS actionengine_pybind11
        EXPORT egltTargets
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib/eglt
        ARCHIVE DESTINATION lib/eglt
)