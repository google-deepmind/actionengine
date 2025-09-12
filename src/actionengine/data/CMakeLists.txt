add_library(actData STATIC)
add_library(act::data ALIAS actData)

target_sources(actData
        PUBLIC
        conversion.h
        types.h
        mimetypes.h
        msgpack.h
        serialization.h
)
target_sources(actData
        PRIVATE
        msgpack.cc
        serialization.cc
        types.cc
)
target_include_directories(
        actData PUBLIC
        ${ACTIONENGINE_PROJECT_TOP_LEVEL}
        ${ACTIONENGINE_INCLUDE_DIR}
        ${ACTIONENGINE_THIRD_PARTY_INCLUDE_DIRS}
)
target_link_libraries(
        actData PRIVATE
        absl::base
        absl::log
        absl::log_internal_check_op
        absl::status
        absl::statusor
        absl::strings
        absl::strings_internal
        absl::time
        cppack
        act::msgpack
)

# ------------------------------------------------------------------

add_library(
        actDataPybind11 STATIC
)
add_library(
        act::data::pybind11 ALIAS
        actDataPybind11
)
target_sources(
        actDataPybind11 PRIVATE
        data_pybind11.cc
)
target_sources(
        actDataPybind11 PUBLIC
        data_pybind11.h
)
target_link_libraries(
        actDataPybind11 PRIVATE
        cppack
        absl::status
        absl::statusor
        act::data
        ${ACTIONENGINE_PYBIND11_DEPENDENCIES}
)
target_include_directories(
        actDataPybind11 PUBLIC
        ${ACTIONENGINE_PYBIND11_INCLUDE_DIRS}
)

install(
        TARGETS actData
        EXPORT actTargets
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib/act
        ARCHIVE DESTINATION lib/act
)