add_library(
    eglt_data STATIC
    chunk_factories.cc
    eg_structs.h
    eg_structs.cc
    serialization.h
)
target_include_directories(
    eglt_data PRIVATE
    ${EGLT_PROJECT_TOP_LEVEL}
    ${EGLT_INCLUDE_DIRS}
)
target_link_libraries(
    eglt_data PUBLIC
    eglt_absl_headers
)
target_precompile_headers(
    eglt_data PUBLIC
    mimetypes.h
)