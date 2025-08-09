add_library(
        actUtilBoostAsio STATIC
        boost_asio_utils.h
        boost_asio_utils.cc
)
add_library(
        act::util::boost_asio ALIAS
        actUtilBoostAsio
)
target_include_directories(
        actUtilBoostAsio PUBLIC
        ${ACTIONENGINE_PROJECT_TOP_LEVEL}
        ${ACTIONENGINE_INCLUDE_DIR}
        ${Boost_INCLUDE_DIRS}
        ${ACTIONENGINE_THIRD_PARTY_INCLUDE_DIRS}
)
target_link_libraries(
        actUtilBoostAsio PRIVATE
        absl::base
        act::concurrency
        Boost::beast
)
# --------------------------------------------------------------------------
add_library(
        actUtil STATIC
        source_location.h
        status_builder.cc
        status_builder.h
        status_macros.h
)
add_library(
        act::util ALIAS
        actUtil
)
target_include_directories(
        actUtil PUBLIC
        ${ACTIONENGINE_PROJECT_TOP_LEVEL}
        ${ACTIONENGINE_INCLUDE_DIR}
        ${ACTIONENGINE_THIRD_PARTY_INCLUDE_DIRS}
)
target_link_libraries(
        actUtil PRIVATE
        absl::base
        absl::inlined_vector
        absl::log
)

add_library(
        actUtilPybind11 STATIC
)
add_library(
        act::util::pybind11 ALIAS
        actUtilPybind11
)
target_sources(
        actUtilPybind11 PRIVATE
        utils_pybind11.cc
)
target_sources(
        actUtilPybind11 PUBLIC
        utils_pybind11.h
)
target_link_libraries(
        actUtilPybind11 PRIVATE
        ${ACTIONENGINE_PYBIND11_DEPENDENCIES}
)
target_include_directories(
        actUtilPybind11 PUBLIC
        ${ACTIONENGINE_PYBIND11_INCLUDE_DIRS}
        ${ACTIONENGINE_PROJECT_TOP_LEVEL}
        ${ACTIONENGINE_INCLUDE_DIR}
        ${ACTIONENGINE_THIRD_PARTY_INCLUDE_DIRS}
)