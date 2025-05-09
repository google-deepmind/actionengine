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
        ${EGLT_ABSL_DEPENDENCIES}
        ${EGLT_PYBIND11_DEPENDENCIES}
)
target_include_directories(
        egltUtilPybind11 PUBLIC
        ${EGLT_PYBIND11_INCLUDE_DIRS}
        ${EGLT_PROJECT_TOP_LEVEL}
        ${EGLT_INCLUDE_DIR}
        ${EGLT_THIRD_PARTY_INCLUDE_DIRS}
)