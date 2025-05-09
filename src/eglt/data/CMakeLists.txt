add_library(egltData STATIC)
add_library(eglt::data ALIAS egltData)

target_sources(egltData
        PUBLIC
        conversion.h
        eg_structs.h
        mimetypes.h
        msgpack.h
        serialization.h
)
target_sources(egltData
        PRIVATE
        eg_structs.cc
)
target_include_directories(
        egltData PUBLIC
        ${EGLT_PROJECT_TOP_LEVEL}
        ${EGLT_INCLUDE_DIR}
        ${EGLT_THIRD_PARTY_INCLUDE_DIRS}
)
target_link_libraries(
        egltData PRIVATE
        cppack
        ${EGLT_ABSL_DEPENDENCIES}
)

# ------------------------------------------------------------------

add_library(
        egltDataPybind11 STATIC
)
add_library(
        eglt::data::pybind11 ALIAS
        egltDataPybind11
)
target_sources(
        egltDataPybind11 PRIVATE
        types_pybind11.cc
)
target_sources(
        egltDataPybind11 PUBLIC
        types_pybind11.h
)
target_link_libraries(
        egltDataPybind11 PRIVATE
        eglt::data
        ${EGLT_PYBIND11_DEPENDENCIES}
)
target_include_directories(
        egltDataPybind11 PUBLIC
        ${EGLT_PYBIND11_INCLUDE_DIRS}
)

install(
        TARGETS egltData
        EXPORT egltTargets
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib/eglt
        ARCHIVE DESTINATION lib/eglt
)