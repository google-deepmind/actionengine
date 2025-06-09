add_executable(
        005-echo-server
        main.cc
)

target_link_libraries(
        005-echo-server PRIVATE
        eglt::actions
        eglt::data
        eglt::nodes
        eglt::service
        eglt::net::websockets
        eglt::net::webrtc
)