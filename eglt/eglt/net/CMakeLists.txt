add_library(
        eglt_net STATIC
        byte_stream.cc
        byte_stream.h
        stream.h
)
add_library(eglt::net ALIAS eglt_net)
target_include_directories(
        eglt_net PRIVATE
        ${EGLT_PROJECT_TOP_LEVEL}
        ${EGLT_INCLUDE_DIRS}
)
target_link_libraries(
        eglt_net PUBLIC
        eglt::absl_headers
        eglt::concurrency
        eglt::data
)