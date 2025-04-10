add_library(
        egltNet STATIC
        byte_stream.cc
        byte_stream.h
        stream.h
)
add_library(eglt::net ALIAS egltNet)
target_include_directories(
        egltNet PRIVATE
        ${EGLT_PROJECT_TOP_LEVEL}
        ${EGLT_INCLUDE_DIR}
        ${EGLT_THIRD_PARTY_INCLUDE_DIRS}
)
target_link_libraries(
        egltNet PUBLIC
        eglt::absl_headers
        eglt::concurrency
        eglt::data
)

install(
        TARGETS egltNet
        EXPORT egltTargets
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib/eglt
        ARCHIVE DESTINATION lib/eglt
)