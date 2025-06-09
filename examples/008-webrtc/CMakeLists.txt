add_executable(
        008-webrtc
        main.cc
)

target_link_libraries(
        008-webrtc PRIVATE
        eglt::actions
        eglt::data
        eglt::nodes
        eglt::service
        eglt::net::webrtc
)