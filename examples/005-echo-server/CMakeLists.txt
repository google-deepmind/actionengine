add_executable(
        005-echo-server
        main.cc
)

target_link_libraries(
        005-echo-server PRIVATE
        absl::flags
        absl::flags_parse
        absl::log
        absl::failure_signal_handler
        eglt::actions
        eglt::data
        eglt::nodes
        eglt::service
        eglt::net::websockets
        eglt::net::webrtc
)