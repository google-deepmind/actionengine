add_library(
        eglt_sdk_websockets STATIC
        websockets.h
        websockets.cc
)
add_library(eglt::sdk::websockets ALIAS eglt_sdk_websockets)
target_include_directories(
        eglt_sdk_websockets PUBLIC
        ${EGLT_PROJECT_TOP_LEVEL}
        ${EGLT_INCLUDE_DIR}
        ${Boost_INCLUDE_DIRS}
        ${EGLT_THIRD_PARTY_INCLUDE_DIRS}
)
target_link_libraries(
        eglt_sdk_websockets PUBLIC
        ${EGLT_ABSL_DEPENDENCIES}
        ${EGLT_PYBIND11_DEPENDENCIES}
        cppack
        eglt::nodes
        Boost::beast
)

# ------------------------------------------------------------------

add_library(
        eglt_sdk_websockets_pybind11 STATIC
)
add_library(
        eglt::sdk::websockets::pybind11 ALIAS
        eglt_sdk_websockets_pybind11
)
target_sources(
        eglt_sdk_websockets_pybind11 PRIVATE
        websockets_pybind11.cc
)
target_sources(
        eglt_sdk_websockets_pybind11 PUBLIC
        websockets_pybind11.h
)
target_link_libraries(
        eglt_sdk_websockets_pybind11 PUBLIC
        eglt::sdk::websockets
        ${EGLT_ABSL_DEPENDENCIES}
        ${EGLT_PYBIND11_DEPENDENCIES}
)
target_include_directories(
        eglt_sdk_websockets_pybind11 PUBLIC
        ${EGLT_PYBIND11_INCLUDE_DIRS}
)

# ------------------------------------------------------------------

add_library(
        eglt_sdk_webrtc STATIC
        webrtc.h
        webrtc.cc
)
add_library(eglt::sdk::webrtc ALIAS eglt_sdk_webrtc)
target_include_directories(
        eglt_sdk_webrtc PUBLIC
        ${EGLT_PROJECT_TOP_LEVEL}
        ${EGLT_INCLUDE_DIR}
        ${Boost_INCLUDE_DIRS}
        ${EGLT_THIRD_PARTY_INCLUDE_DIRS}

)
target_link_libraries(
        eglt_sdk_webrtc PUBLIC
        ${EGLT_ABSL_DEPENDENCIES}
        ${EGLT_PYBIND11_DEPENDENCIES}
        cppack
        eglt::nodes
        Boost::beast
        LibDataChannel::LibDataChannel
)