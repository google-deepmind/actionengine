add_library(
        actNet STATIC
        client.cc
        stream.h
)
add_library(act::net ALIAS actNet)
target_include_directories(
        actNet PUBLIC
        ${ACTIONENGINE_PROJECT_TOP_LEVEL}
        ${ACTIONENGINE_INCLUDE_DIR}
)
target_link_libraries(
        actNet PUBLIC
        act::concurrency
        act::data
)

# -----------------------------------------------------------------------------

add_subdirectory(webrtc)
add_subdirectory(websockets)

# -----------------------------------------------------------------------------

install(
        TARGETS actNet
        EXPORT actTargets
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib/act
        ARCHIVE DESTINATION lib/act
)