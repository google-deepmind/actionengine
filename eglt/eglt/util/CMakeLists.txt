add_library(eglt_util INTERFACE)
add_library(eglt::util ALIAS eglt_util)
set_target_properties(eglt_util PROPERTIES
        LANGUAGE CXX
        LINKER_LANGUAGE CXX
)
target_precompile_headers(
        eglt_util INTERFACE map_util.h random.h
)
target_include_directories(
        eglt_util
        INTERFACE
        ${EGLT_PROJECT_TOP_LEVEL}
        ${EGLT_INCLUDE_DIR}
        ${EGLT_THIRD_PARTY_INCLUDE_DIRS}
)
target_link_libraries(
        eglt_util
        INTERFACE
        eglt::absl_headers
)