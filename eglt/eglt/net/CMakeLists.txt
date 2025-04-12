add_library(
        egltNet INTERFACE
        stream.h
)
add_library(eglt::net ALIAS egltNet)
target_include_directories(
        egltNet INTERFACE
        ${EGLT_PROJECT_TOP_LEVEL}
        ${EGLT_INCLUDE_DIR}
)
target_link_libraries(
        egltNet INTERFACE
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