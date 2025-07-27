add_executable(
        001-bidi-actions
        bidi_actions.cc
)

target_link_libraries(
        001-bidi-actions PRIVATE
        absl::flags
        absl::flags_parse
        absl::log
        absl::failure_signal_handler
        eglt::actions
        eglt::data
        eglt::nodes
        eglt::service
        eglt::net::websockets
)