find_package(Protobuf REQUIRED)
set(PROTO_FILES
        types.proto)
protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${PROTO_FILES})

add_library(actProto STATIC ${PROTO_SRCS} ${PROTO_HDRS})
add_library(act::proto ALIAS actProto)

target_include_directories(actProto PUBLIC
        ${Protobuf_INCLUDE_DIRS}
        ${CMAKE_CURRENT_BINARY_DIR})


target_include_directories(actProto PRIVATE
        ${ACTIONENGINE_THIRD_PARTY_INCLUDE_DIRS}
        ${ACTIONENGINE_INCLUDE_DIR})

target_link_libraries(actProto PUBLIC
        utf8_range::utf8_range
        ${Protobuf_LIBRARIES}
)