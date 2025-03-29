add_library(
    eglt_concurrency INTERFACE
)
target_precompile_headers(
    eglt_concurrency INTERFACE
    not_implemented.h
    concurrency.h
    implementation.h
)
target_include_directories(
    eglt_concurrency INTERFACE
    ${EGLT_PROJECT_TOP_LEVEL}
    ${EGLT_INCLUDE_DIRS}
)
target_link_libraries(
    eglt_concurrency INTERFACE
    eglt_absl_headers
)