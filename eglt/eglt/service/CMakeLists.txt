add_library(
    eglt_service STATIC
    service.cc
    service.h
    session.cc
    session.h
)
target_include_directories(
    eglt_service PRIVATE
    ${EGLT_PROJECT_TOP_LEVEL}
    ${EGLT_INCLUDE_DIRS}
)
target_link_libraries(
    eglt_service PUBLIC
    eglt_absl_headers
    eglt_concurrency
    eglt_data
)