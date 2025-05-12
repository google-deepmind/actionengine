add_executable(
        004-connections-through-nodes
        main.cc
)

target_link_libraries(
        004-connections-through-nodes PRIVATE
        eglt::data
        eglt::nodes
        eglt::actions
        eglt::sdk::serving::websockets
        eglt::service
)