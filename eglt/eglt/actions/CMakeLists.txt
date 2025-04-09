add_library(
        eglt_actions STATIC
        action.cc
        action.h
)
add_library(
        eglt::actions ALIAS
        eglt_actions
)
target_include_directories(
        eglt_actions
        PRIVATE
        ${EGLT_PROJECT_TOP_LEVEL}
        ${EGLT_INCLUDE_DIR}
        ${EGLT_THIRD_PARTY_INCLUDE_DIRS}
)
target_link_libraries(
        eglt_actions
        PUBLIC
        eglt::absl_headers
        eglt::data
        eglt::net
        eglt::nodes
        eglt::util
)