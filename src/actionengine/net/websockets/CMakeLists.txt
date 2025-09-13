add_library(
        actNetWebsockets STATIC
        fiber_aware_websocket_stream.h
        fiber_aware_websocket_stream.cc
        server.h
        server.cc
        wire_stream.cc
        wire_stream.h
)
add_library(act::net::websockets ALIAS actNetWebsockets)
target_include_directories(
        actNetWebsockets PUBLIC
        ${ACTIONENGINE_PROJECT_TOP_LEVEL}
        ${ACTIONENGINE_INCLUDE_DIR}
        ${ACTIONENGINE_THIRD_PARTY_INCLUDE_DIRS}
)
target_link_libraries(
        actNetWebsockets PRIVATE
)
target_link_libraries(
        actNetWebsockets PUBLIC
        cppack
        act::nodes
        Boost::asio
        Boost::beast
        act::util::boost_asio
)

# ------------------------------------------------------------------

add_library(
        actNetWebsocketsPybind11 STATIC
)
add_library(
        act::net::websockets::pybind11 ALIAS
        actNetWebsocketsPybind11
)
target_sources(
        actNetWebsocketsPybind11 PRIVATE
        websockets_pybind11.cc
)
target_sources(
        actNetWebsocketsPybind11 PUBLIC
        websockets_pybind11.h
)
target_link_libraries(
        actNetWebsocketsPybind11 PUBLIC
        act::net::websockets
        ${ACTIONENGINE_PYBIND11_DEPENDENCIES}
)
target_include_directories(
        actNetWebsocketsPybind11 PUBLIC
        ${ACTIONENGINE_PYBIND11_INCLUDE_DIRS}
)
