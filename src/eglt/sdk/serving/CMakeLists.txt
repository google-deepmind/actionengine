add_library(
        eglt_sdk_serving_websockets STATIC
        websockets.h
        websockets.cc
)
add_library(eglt::sdk::serving::websockets ALIAS eglt_sdk_serving_websockets)
target_include_directories(
        eglt_sdk_serving_websockets PUBLIC
        ${EGLT_PROJECT_TOP_LEVEL}
        ${EGLT_INCLUDE_DIR}
        ${Boost_INCLUDE_DIRS}
        ${EGLT_THIRD_PARTY_INCLUDE_DIRS}
)
target_link_libraries(
        eglt_sdk_serving_websockets PUBLIC
        ${EGLT_ABSL_DEPENDENCIES}
        ${EGLT_PYBIND11_DEPENDENCIES}
        cppack
        eglt::nodes
        Boost::beast
)

# ------------------------------------------------------------------

add_library(
        eglt_sdk_serving_websockets_pybind11 STATIC
)
add_library(
        eglt::sdk::serving::websockets::pybind11 ALIAS
        eglt_sdk_serving_websockets_pybind11
)
target_sources(
        eglt_sdk_serving_websockets_pybind11 PRIVATE
        websockets_pybind11.cc
)
target_sources(
        eglt_sdk_serving_websockets_pybind11 PUBLIC
        websockets_pybind11.h
)
target_link_libraries(
        eglt_sdk_serving_websockets_pybind11 PUBLIC
        eglt::sdk::serving::websockets
        ${EGLT_ABSL_DEPENDENCIES}
        ${EGLT_PYBIND11_DEPENDENCIES}
)
target_include_directories(
        eglt_sdk_serving_websockets_pybind11 PUBLIC
        ${EGLT_PYBIND11_INCLUDE_DIRS}
)