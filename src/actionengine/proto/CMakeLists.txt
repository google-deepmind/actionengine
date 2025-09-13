add_library(actProto STATIC converters.h converters.cc)

find_package(Protobuf REQUIRED)
set(PROTO_FILES types.proto)

protobuf_generate(
        LANGUAGE PYTHON
        TARGET actProto
        PROTOS ${PROTO_FILES}
        PROTOC_OUT_DIR ${CMAKE_CURRENT_BINARY_DIR}
)
protobuf_generate(
        LANGUAGE CPP
        TARGET actProto
        PROTOS ${PROTO_FILES}
        PROTOC_OUT_DIR ${CMAKE_CURRENT_BINARY_DIR}
)
message(STATUS "Generated Python protobuf files: ${PY_PROTO_SRCS}")

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
        absl::status
        act::data
        act::util
)