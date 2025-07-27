add_library(
        egltNetWebsockets STATIC
        fiber_aware_websocket_stream.h
        fiber_aware_websocket_stream.cc
        websockets.h
        websockets.cc
)
add_library(eglt::net::websockets ALIAS egltNetWebsockets)
target_include_directories(
        egltNetWebsockets PUBLIC
        ${EGLT_PROJECT_TOP_LEVEL}
        ${EGLT_INCLUDE_DIR}
        #        ${Boost_INCLUDE_DIRS}
        ${EGLT_THIRD_PARTY_INCLUDE_DIRS}
)
target_link_libraries(
        egltNetWebsockets PRIVATE
)
target_link_libraries(
        egltNetWebsockets PUBLIC
        cppack
        eglt::nodes
        Boost::asio
        Boost::beast
        eglt::util::boost_asio
)

# ------------------------------------------------------------------

add_library(
        egltNetWebsocketsPybind11 STATIC
)
add_library(
        eglt::net::websockets::pybind11 ALIAS
        egltNetWebsocketsPybind11
)
target_sources(
        egltNetWebsocketsPybind11 PRIVATE
        websockets_pybind11.cc
)
target_sources(
        egltNetWebsocketsPybind11 PUBLIC
        websockets_pybind11.h
)
target_link_libraries(
        egltNetWebsocketsPybind11 PUBLIC
        eglt::net::websockets
        ${EGLT_PYBIND11_DEPENDENCIES}
)
target_include_directories(
        egltNetWebsocketsPybind11 PUBLIC
        ${EGLT_PYBIND11_INCLUDE_DIRS}
)
