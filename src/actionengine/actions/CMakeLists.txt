add_library(
        actActions STATIC
        action.cc
        action.h
        registry.cc
        registry.h
        schema.cc
        schema.h
)
add_library(
        act::actions ALIAS
        actActions
)
target_include_directories(
        actActions
        PUBLIC
        ${ACTIONENGINE_PROJECT_TOP_LEVEL}
        ${ACTIONENGINE_INCLUDE_DIR}
        ${ACTIONENGINE_THIRD_PARTY_INCLUDE_DIRS}
)
target_link_libraries(
        actActions PRIVATE
        absl::base
        absl::log
        absl::log_internal_check_op
        absl::status
        absl::statusor
        absl::strings
        absl::strings_internal
)
target_link_libraries(
        actActions
        PUBLIC
        act::data
        act::net
        act::nodes
        act::stores
)

# ------------------------------------------------------------------

add_library(
        actActionsPybind11 STATIC
)
add_library(
        act::actions::pybind11 ALIAS
        actActionsPybind11
)
target_sources(
        actActionsPybind11 PRIVATE
        actions_pybind11.cc
)
target_sources(
        actActionsPybind11 PUBLIC
        actions_pybind11.h
)
target_link_libraries(
        actActionsPybind11 PRIVATE
        absl::log
        act::actions
        ${ACTIONENGINE_PYBIND11_DEPENDENCIES}
)
target_include_directories(
        actActionsPybind11 PUBLIC
        ${ACTIONENGINE_PYBIND11_INCLUDE_DIRS}
)

install(
        TARGETS actActions
        EXPORT actTargets
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib/act
        ARCHIVE DESTINATION lib/act
)