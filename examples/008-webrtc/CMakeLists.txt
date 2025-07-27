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
        eglt::actions
        eglt::data
        eglt::msgpack
        eglt::nodes
        eglt::service
        eglt::net::webrtc
)