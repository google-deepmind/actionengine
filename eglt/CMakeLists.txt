add_library(eglt_absl_headers INTERFACE)
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
        absl::status
        absl::statusor
        absl::strings
        absl::time
)
target_include_directories(
        eglt_absl_headers
        INTERFACE
        ${EGLT_INCLUDE_DIR}
        ${EGLT_THIRD_PARTY_INSTALL_ROOT}/abseil-cpp/include
)
target_precompile_headers(eglt_absl_headers INTERFACE ${EGLT_INCLUDE_DIR}/eglt/absl_headers.h)

add_subdirectory(eglt/actions)
add_subdirectory(eglt/concurrency)
add_subdirectory(eglt/data)
add_subdirectory(eglt/net)
add_subdirectory(eglt/nodes)
add_subdirectory(eglt/service)
add_subdirectory(eglt/util)