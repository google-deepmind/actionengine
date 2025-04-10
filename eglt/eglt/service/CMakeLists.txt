add_library(
        egltService STATIC
        service.cc
        service.h
        session.cc
        session.h
)
add_library(eglt::service ALIAS egltService)
target_include_directories(
        egltService PRIVATE
        ${EGLT_PROJECT_TOP_LEVEL}
        ${EGLT_INCLUDE_DIR}
        ${EGLT_THIRD_PARTY_INCLUDE_DIRS}
)
target_link_libraries(
        egltService PUBLIC
        eglt::absl_headers
        eglt::concurrency
        eglt::data
)

install(
        TARGETS egltService
        EXPORT egltTargets
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib/eglt
        ARCHIVE DESTINATION lib/eglt
)