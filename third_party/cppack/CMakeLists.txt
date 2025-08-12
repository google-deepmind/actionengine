add_library(
        cppack STATIC
        cppack/msgpack.cc
        cppack/msgpack.h
)

target_link_libraries(cppack
        PRIVATE
        absl::status
        absl::statusor
)

target_include_directories(cppack
        PUBLIC
        ${CMAKE_CURRENT_LIST_DIR}
)

#target_include_directories(cppack INTERFACE
#        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
#        $<INSTALL_INTERFACE:include>
#)
