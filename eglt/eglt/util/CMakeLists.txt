add_library(egltUtil STATIC)
add_library(eglt::util ALIAS egltUtil)
set_target_properties(egltUtil PROPERTIES
        LANGUAGE CXX
        LINKER_LANGUAGE CXX
)
target_sources(
        egltUtil PUBLIC map_util.h random.h
)
target_include_directories(
        egltUtil
        PUBLIC
        ${EGLT_PROJECT_TOP_LEVEL}
        ${EGLT_INCLUDE_DIR}
        ${EGLT_THIRD_PARTY_INCLUDE_DIRS}
)
target_link_libraries(
        egltUtil
        PUBLIC
        eglt::absl_headers
)

install(
        TARGETS egltUtil
        EXPORT egltTargets
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib/eglt
        ARCHIVE DESTINATION lib/eglt
)