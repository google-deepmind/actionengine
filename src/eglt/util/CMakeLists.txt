add_library(egltUtil STATIC)
add_library(eglt::util ALIAS egltUtil)
set_target_properties(egltUtil PROPERTIES
        LANGUAGE CXX
        LINKER_LANGUAGE CXX
)
target_sources(
        egltUtil PUBLIC map_util.h platform.h random.h
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

# ------------------------------------------------------------------

add_library(
        egltUtilPybind11 STATIC
)
add_library(
        eglt::util::pybind11 ALIAS
        egltUtilPybind11
)
target_sources(
        egltUtilPybind11 PRIVATE
        utils_pybind11.cc
)
target_sources(
        egltUtilPybind11 PUBLIC
        utils_pybind11.h
)
target_link_libraries(
        egltUtilPybind11 PRIVATE
        eglt::util
        eglt::pybind11_headers
)

install(
        TARGETS egltUtil
        EXPORT egltTargets
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib/eglt
        ARCHIVE DESTINATION lib/eglt
)