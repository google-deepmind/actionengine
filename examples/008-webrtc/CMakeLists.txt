add_executable(
        008-webrtc
        main.cc
)

target_link_libraries(
        008-webrtc PRIVATE
        eglt::actions
        eglt::data
        eglt::msgpack
        eglt::nodes
        eglt::service
        eglt::net::webrtc
)