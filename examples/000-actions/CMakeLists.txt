add_executable(
        000-actions
        main.cc
)

target_link_libraries(
        000-actions PRIVATE
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