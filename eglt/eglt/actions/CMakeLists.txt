add_library(
        egltActions STATIC
        action.cc
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

install(
        TARGETS egltActions
        EXPORT egltTargets
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib/eglt
        ARCHIVE DESTINATION lib/eglt
)