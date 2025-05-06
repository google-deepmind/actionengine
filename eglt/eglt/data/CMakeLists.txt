add_library(egltData INTERFACE)
add_library(eglt::data ALIAS egltData)

target_sources(egltData
        INTERFACE
        conversion.h
        eg_structs.h
        mimetypes.h
        msgpack.h
        serialization.h
)
target_include_directories(
        egltData INTERFACE
        ${EGLT_PROJECT_TOP_LEVEL}
        ${EGLT_INCLUDE_DIR}
        ${EGLT_THIRD_PARTY_INCLUDE_DIRS}
)
target_link_libraries(
        egltData INTERFACE
        cppack
        eglt::absl_headers
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
        eglt::pybind11_headers
)

install(
        TARGETS egltData
        EXPORT egltTargets
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib/eglt
        ARCHIVE DESTINATION lib/eglt
)