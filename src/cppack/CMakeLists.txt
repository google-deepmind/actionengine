add_library(cppack INTERFACE)

target_link_libraries(cppack
        INTERFACE
        absl::status
        absl::statusor
)

target_include_directories(cppack INTERFACE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)
