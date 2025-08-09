add_executable(
        008-webrtc
        main.cc
)

target_link_libraries(
        008-webrtc PRIVATE
        absl::flags
        absl::flags_parse
        absl::log
        absl::failure_signal_handler
        act::actions
        act::data
        act::msgpack
        act::nodes
        act::service
        act::net::webrtc
)