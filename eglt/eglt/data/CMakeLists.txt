add_library(egltData STATIC)
add_library(eglt::data ALIAS egltData)

target_sources(egltData
        PUBLIC
        chunk_factories.cc
        eg_structs.cc
        eg_structs.h
        mimetypes.h
        serialization.h
)
target_sources(egltData
        PUBLIC
        string_helpers.cc
)
target_include_directories(
        egltData PRIVATE
        ${EGLT_PROJECT_TOP_LEVEL}
        ${EGLT_INCLUDE_DIR}
        ${EGLT_THIRD_PARTY_INCLUDE_DIRS}
)
target_link_libraries(
        egltData PUBLIC
        cppack
        eglt::absl_headers
)

install(
        TARGETS egltData
        EXPORT egltTargets
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib/eglt
        ARCHIVE DESTINATION lib/eglt
)