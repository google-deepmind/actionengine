add_library(
        eglt_actions STATIC
        action.h
        action.cc
)
add_library(
        eglt::actions ALIAS
        eglt_actions
)
target_include_directories(
        eglt_actions
        PRIVATE
        ${EGLT_PROJECT_TOP_LEVEL}
        ${EGLT_INCLUDE_DIRS}
)
target_link_libraries(
        eglt_actions
        PUBLIC
        eglt_absl_headers
        eglt_data
        eglt_net
        eglt_nodes
        eglt_util
)