add_library(egltUtil INTERFACE)
add_library(eglt::util ALIAS egltUtil)
set_target_properties(egltUtil PROPERTIES
        LANGUAGE CXX
        LINKER_LANGUAGE CXX
)
target_precompile_headers(
        egltUtil INTERFACE map_util.h random.h
)
target_include_directories(
        egltUtil
        INTERFACE
        ${EGLT_PROJECT_TOP_LEVEL}
        ${EGLT_INCLUDE_DIR}
        ${EGLT_THIRD_PARTY_INCLUDE_DIRS}
)
target_link_libraries(
        egltUtil
        INTERFACE
        eglt::absl_headers
)

install(
        TARGETS egltUtil
        EXPORT egltTargets
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib/eglt
        ARCHIVE DESTINATION lib/eglt
)