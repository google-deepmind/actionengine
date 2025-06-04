add_executable(
        001-bidi-actions
        bidi_actions.cc
)

target_link_libraries(
        001-bidi-actions PRIVATE
        eglt::actions
        eglt::data
        eglt::nodes
        eglt::service
        eglt::sdk::websockets
)