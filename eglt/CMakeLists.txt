add_library(eglt_absl_headers INTERFACE)
add_library(eglt::absl_headers ALIAS eglt_absl_headers)
set_target_properties(eglt_absl_headers PROPERTIES
        LANGUAGE CXX
        LINKER_LANGUAGE CXX
)
target_link_libraries(eglt_absl_headers
        INTERFACE
        absl::base
        absl::flags
        absl::flat_hash_map
        absl::log
        absl::log_initialize
        absl::log_internal_check_op
        absl::random_random
        absl::status
        absl::status_matchers
        absl::statusor
        absl::strings
        absl::strings_internal
        absl::time
)
target_include_directories(
        eglt_absl_headers
        INTERFACE
        ${EGLT_INCLUDE_DIR}
        ${EGLT_THIRD_PARTY_INSTALL_ROOT}/abseil-cpp/include
)
target_precompile_headers(eglt_absl_headers INTERFACE ${EGLT_INCLUDE_DIR}/eglt/absl_headers.h)

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
add_subdirectory(eglt/util)