string(FIND ${CMAKE_CXX_COMPILER_ID} "Clang" USING_CLANG)
message(STATUS "CMAKE_CXX_COMPILER_ID: ${CMAKE_CXX_COMPILER_ID}")
if (USING_CLANG GREATER -1)
    add_compile_options(-Wall -Wthread-safety -Werror=thread-safety -Wnullability-completeness)
endif ()

#set(ACTIONENGINE_IWYU_PATH "/Users/helenapankov/dev/include-what-you-use/build/bin/include-what-you-use")

if (DEFINED ENV{ACTIONENGINE_IWYU_PATH})
    set(ACTIONENGINE_IWYU_PATH "$ENV{ACTIONENGINE_IWYU_PATH}")
endif ()

if (DEFINED ENV{INCLUDE_WHAT_YOU_USE})
    set(ACTIONENGINE_IWYU_PATH "$ENV{INCLUDE_WHAT_YOU_USE}")
endif ()

if (DEFINED ENV{IWYU_PATH})
    set(ACTIONENGINE_IWYU_PATH "$ENV{IWYU_PATH}")
endif ()

if (DEFINED ACTIONENGINE_IWYU_PATH)
    if (APPLE)
        set(ACTIONENGINE_IWYU_PATH "xcrun;${ACTIONENGINE_IWYU_PATH}")
    endif ()
    set(CMAKE_CXX_INCLUDE_WHAT_YOU_USE
            "${ACTIONENGINE_IWYU_PATH};-w;-std=c++20;-Xiwyu;--verbose=1;-Xiwyu;
            --comment_style=none;-Xiwyu;--no_fwd_decls")
endif ()

# ------------------------------------------------------------------

set(ACTIONENGINE_PYBIND11_DEPENDENCIES
        pybind11::module
        pybind11::embed
        pybind11_abseil::status_casters
        pybind11_abseil::absl_casters
        pybind11_abseil::import_status_module
        pybind11_abseil::statusor_caster
)
set(ACTIONENGINE_PYBIND11_DEPENDENCIES ${ACTIONENGINE_PYBIND11_DEPENDENCIES} PARENT_SCOPE)

set(
        ACTIONENGINE_PYBIND11_INCLUDE_DIRS
        ${ACTIONENGINE_THIRD_PARTY_INSTALL_ROOT}/pybind11_abseil
        ${Python_INCLUDE_DIRS}
        ${ACTIONENGINE_PYBIND11_TOP_LEVEL}
)
set(ACTIONENGINE_PYBIND11_INCLUDE_DIRS ${ACTIONENGINE_PYBIND11_INCLUDE_DIRS} PARENT_SCOPE)

# ------------------------------------------------------------------

add_library(
        act_testing INTERFACE
)
add_library(
        act::testing ALIAS act_testing
)
set_target_properties(act_testing PROPERTIES
        LANGUAGE CXX
        LINKER_LANGUAGE CXX
)
target_include_directories(
        act_testing
        INTERFACE
        ${ACTIONENGINE_INCLUDE_DIR}
        ${ACTIONENGINE_THIRD_PARTY_INSTALL_ROOT}/googletest/include
)

add_subdirectory(actionengine/actions)
add_subdirectory(actionengine/concurrency)
add_subdirectory(actionengine/data)
add_subdirectory(actionengine/msgpack)
add_subdirectory(actionengine/net)
add_subdirectory(actionengine/nodes)
add_subdirectory(actionengine/redis)
add_subdirectory(actionengine/service)
add_subdirectory(actionengine/stores)
add_subdirectory(actionengine/util)
add_subdirectory(pybind11_abseil_module)

# ------------------------------------------------------------------

pybind11_add_module(
        actionengine_pybind11
        actionengine/actionengine_pybind11.cc
)
add_library(
        act::pybind11 ALIAS
        actionengine_pybind11
)

target_link_libraries(
        actionengine_pybind11 PRIVATE
        absl::failure_signal_handler
)

target_link_libraries(
        actionengine_pybind11 PRIVATE
        absl::base
        absl::flags
        absl::flags_parse
        absl::log
        absl::log_internal_check_op
        absl::status
        absl::statusor
        absl::strings
        absl::strings_internal
        absl::failure_signal_handler
        act::data
        act::data::pybind11
        act::actions
        act::actions::pybind11
        act::concurrency
        act::net
        act::service
        act::service::pybind11
        act::stores
        act::stores::pybind11
        act::nodes
        act::nodes::pybind11
        act::net::webrtc
        act::net::webrtc::pybind11
        act::net::websockets
        act::net::websockets::pybind11
        act::redis
        act::redis::pybind11
        act::util::pybind11
        Python::Module
        Python::Python
        ${ACTIONENGINE_PYBIND11_DEPENDENCIES}
)

target_include_directories(
        actionengine_pybind11 PRIVATE
        ${ACTIONENGINE_INCLUDE_DIR}
        ${ACTIONENGINE_THIRD_PARTY_INCLUDE_DIRS}
        ${ACTIONENGINE_THIRD_PARTY_INSTALL_ROOT}/pybind11_abseil
        ${ACTIONENGINE_PROJECT_TOP_LEVEL}
        ${ACTIONENGINE_PYBIND11_INCLUDE_DIRS}
        ${ACTIONENGINE_PYBIND11_TOP_LEVEL}
)

install(
        TARGETS actionengine_pybind11
        EXPORT actTargets
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib/act
        ARCHIVE DESTINATION lib/act
)