add_library(eglt_data STATIC)
add_library(eglt::data ALIAS eglt_data)

target_sources(eglt_data
        PRIVATE
        chunk_factories.cc
        string_helpers.cc
        eg_structs.cc
        eg_structs.h
        mimetypes.h
        serialization.h
)
target_include_directories(
        eglt_data PRIVATE
        ${EGLT_PROJECT_TOP_LEVEL}
        ${EGLT_INCLUDE_DIRS}
)
target_link_libraries(
        eglt_data PUBLIC
        cppack
        eglt::absl_headers
)