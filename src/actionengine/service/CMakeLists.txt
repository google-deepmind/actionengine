add_library(
        actService STATIC
        service.cc
        service.h
        session.cc
        session.h
)
add_library(act::service ALIAS actService)
target_include_directories(
        actService PRIVATE
        ${ACTIONENGINE_PROJECT_TOP_LEVEL}
        ${ACTIONENGINE_INCLUDE_DIR}
        ${ACTIONENGINE_THIRD_PARTY_INCLUDE_DIRS}
)
target_link_libraries(
        actService PRIVATE
)
target_link_libraries(
        actService PUBLIC
        act::actions
        act::concurrency
        act::data
)

target_link_libraries(
        actService PRIVATE
        absl::base
        absl::log
        absl::log_internal_check_op
        absl::status
        absl::statusor
        absl::time
)

# ------------------------------------------------------------------

add_library(
        actServicePybind11 STATIC
)
add_library(
        act::service::pybind11 ALIAS
        actServicePybind11
)
target_sources(
        actServicePybind11 PRIVATE
        service_pybind11.cc
)
target_sources(
        actServicePybind11 PUBLIC
        service_pybind11.h
)
target_link_libraries(
        actServicePybind11 PRIVATE
        act::service
        ${ACTIONENGINE_PYBIND11_DEPENDENCIES}
)
target_include_directories(
        actServicePybind11 PUBLIC
        ${ACTIONENGINE_PYBIND11_INCLUDE_DIRS}
)

install(
        TARGETS actService
        EXPORT actTargets
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib/act
        ARCHIVE DESTINATION lib/act
)