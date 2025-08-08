add_library(
        egltActions STATIC
        action.cc
        action.h
        registry.cc
        registry.h
        schema.cc
        schema.h
)
add_library(
        eglt::actions ALIAS
        egltActions
)
target_include_directories(
        egltActions
        PUBLIC
        ${EGLT_PROJECT_TOP_LEVEL}
        ${EGLT_INCLUDE_DIR}
        ${EGLT_THIRD_PARTY_INCLUDE_DIRS}
)
target_link_libraries(
        egltActions PRIVATE
        absl::base
        absl::log
        absl::log_internal_check_op
        absl::status
        absl::statusor
        absl::strings
        absl::strings_internal
)
target_link_libraries(
        egltActions
        PUBLIC
        eglt::data
        eglt::net
        eglt::nodes
)

# ------------------------------------------------------------------

add_library(
        egltActionsPybind11 STATIC
)
add_library(
        eglt::actions::pybind11 ALIAS
        egltActionsPybind11
)
target_sources(
        egltActionsPybind11 PRIVATE
        actions_pybind11.cc
)
target_sources(
        egltActionsPybind11 PUBLIC
        actions_pybind11.h
)
target_link_libraries(
        egltActionsPybind11 PRIVATE
        eglt::actions
        ${EGLT_PYBIND11_DEPENDENCIES}
)
target_include_directories(
        egltActionsPybind11 PUBLIC
        ${EGLT_PYBIND11_INCLUDE_DIRS}
)

install(
        TARGETS egltActions
        EXPORT egltTargets
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib/eglt
        ARCHIVE DESTINATION lib/eglt
)