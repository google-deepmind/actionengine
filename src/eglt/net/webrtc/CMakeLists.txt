add_library(
        egltNetWebrtc STATIC
        signalling.h
        signalling.cc
        webrtc.h
        webrtc.cc
)
add_library(eglt::net::webrtc ALIAS egltNetWebrtc)
target_include_directories(
        egltNetWebrtc PUBLIC
        ${EGLT_PROJECT_TOP_LEVEL}
        ${EGLT_INCLUDE_DIR}
        ${EGLT_THIRD_PARTY_INCLUDE_DIRS}

)
target_link_libraries(
        egltNetWebrtc PUBLIC
        Boost::json
        Boost::beast
        LibDataChannel::LibDataChannel
)
target_link_libraries(
        egltNetWebrtc PRIVATE
        cppack
        eglt::concurrency
        eglt::nodes
        eglt::stores
        egltNetWebsockets
)

# ------------------------------------------------------------------

add_library(
        egltNetWebrtcPybind11 STATIC
)
add_library(
        eglt::net::webrtc::pybind11 ALIAS
        egltNetWebrtcPybind11
)
target_sources(
        egltNetWebrtcPybind11 PRIVATE
        webrtc_pybind11.cc
)
target_sources(
        egltNetWebrtcPybind11 PUBLIC
        webrtc_pybind11.h
)
target_link_libraries(
        egltNetWebrtcPybind11 PUBLIC
        eglt::net::webrtc
        ${EGLT_PYBIND11_DEPENDENCIES}
)
target_link_libraries(
        egltNetWebrtcPybind11 PRIVATE
        eglt::concurrency
)
target_include_directories(
        egltNetWebrtcPybind11 PUBLIC
        ${EGLT_PYBIND11_INCLUDE_DIRS}
)