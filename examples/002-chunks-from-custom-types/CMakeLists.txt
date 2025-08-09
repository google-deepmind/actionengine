add_executable(
        002-chunks-from-custom-types
        main.cc
)

target_link_libraries(
        002-chunks-from-custom-types PRIVATE
        absl::flags
        absl::flags_parse
        absl::log
        absl::failure_signal_handler
        act::data
        act::nodes
        act::redis
)