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
        egltService PRIVATE
)
target_link_libraries(
        egltService PUBLIC
        eglt::actions
        eglt::concurrency
        eglt::data
)

target_link_libraries(
        egltService PRIVATE
        absl::base
        absl::log
        absl::log_internal_check_op
        absl::status
        absl::statusor
        absl::time
)

# ------------------------------------------------------------------

add_library(
        egltServicePybind11 STATIC
)
add_library(
        eglt::service::pybind11 ALIAS
        egltServicePybind11
)
target_sources(
        egltServicePybind11 PRIVATE
        service_pybind11.cc
)
target_sources(
        egltServicePybind11 PUBLIC
        service_pybind11.h
)
target_link_libraries(
        egltServicePybind11 PRIVATE
        eglt::service
        ${EGLT_PYBIND11_DEPENDENCIES}
)
target_include_directories(
        egltServicePybind11 PUBLIC
        ${EGLT_PYBIND11_INCLUDE_DIRS}
)

install(
        TARGETS egltService
        EXPORT egltTargets
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib/eglt
        ARCHIVE DESTINATION lib/eglt
)