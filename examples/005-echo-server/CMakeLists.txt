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
        act::actions
        act::data
        act::nodes
        act::service
        act::net::websockets
        act::net::webrtc
)