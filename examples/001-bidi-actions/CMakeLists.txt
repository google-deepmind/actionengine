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
        act::actions
        act::data
        act::nodes
        act::service
        act::net::websockets
)