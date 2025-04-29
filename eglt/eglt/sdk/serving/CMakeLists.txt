add_library(
        eglt_sdk_serving_websockets STATIC
        websockets.cc
        websockets.h
)
add_library(eglt::sdk::serving::websockets ALIAS eglt_sdk_serving_websockets)
target_include_directories(
        eglt_sdk_serving_websockets PRIVATE
        ${EGLT_PROJECT_TOP_LEVEL}
        ${EGLT_INCLUDE_DIR}
        ${Boost_INCLUDE_DIRS}
        ${EGLT_THIRD_PARTY_INCLUDE_DIRS}
)
target_link_libraries(
        eglt_sdk_serving_websockets PUBLIC
        eglt::absl_headers
        eglt::nodes
        Boost::beast
)

add_executable(
        eglt_ws_playground
        playground.cc
)
target_include_directories(
        eglt_ws_playground PRIVATE
        ${EGLT_PROJECT_TOP_LEVEL}
        ${EGLT_INCLUDE_DIR}
        ${EGLT_THIRD_PARTY_INCLUDE_DIRS}
)
target_link_libraries(
        eglt_ws_playground PUBLIC
        eglt::actions
        eglt::sdk::serving::websockets
        eglt::service
)