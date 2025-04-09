add_library(
        eglt_service STATIC
        service.cc
        service.h
        session.cc
        session.h
)
add_library(eglt::service ALIAS eglt_service)
target_include_directories(
        eglt_service PRIVATE
        ${EGLT_PROJECT_TOP_LEVEL}
        ${EGLT_INCLUDE_DIR}
        ${EGLT_THIRD_PARTY_INCLUDE_DIRS}
)
target_link_libraries(
        eglt_service PUBLIC
        eglt::absl_headers
        eglt::concurrency
        eglt::data
)