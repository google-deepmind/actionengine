add_executable(
        002-chunks-from-custom-types
        main.cc
)

target_link_libraries(
        002-chunks-from-custom-types PRIVATE
        eglt::core
)