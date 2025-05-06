add_library(
        egltActions STATIC
        action.h
)
add_library(
        eglt::actions ALIAS
        egltActions
)
target_include_directories(
        egltActions
        PRIVATE
        ${EGLT_PROJECT_TOP_LEVEL}
        ${EGLT_INCLUDE_DIR}
        ${EGLT_THIRD_PARTY_INCLUDE_DIRS}
)
target_link_libraries(
        egltActions
        PUBLIC
        eglt::absl_headers
        eglt::data
        eglt::net
        eglt::nodes
        eglt::util
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
        eglt::pybind11_headers
)

install(
        TARGETS egltActions
        EXPORT egltTargets
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib/eglt
        ARCHIVE DESTINATION lib/eglt
)