add_library(
        actConcurrency STATIC
)
add_library(act::concurrency ALIAS actConcurrency)
target_sources(
        actConcurrency PRIVATE
        concurrency.cc
)
target_sources(
        actConcurrency PUBLIC
        concurrency.h
)
target_include_directories(
        actConcurrency PUBLIC
        ${ACTIONENGINE_PROJECT_TOP_LEVEL}
        ${ACTIONENGINE_INCLUDE_DIR}
        ${ACTIONENGINE_THIRD_PARTY_INCLUDE_DIRS}
)
target_include_directories(
        actConcurrency PUBLIC
        ${G3FIBER_INCLUDE_DIRS}
)
target_link_libraries(
        actConcurrency PUBLIC
        G3Fiber
)
target_link_libraries(
        actConcurrency PRIVATE
        absl::base
        absl::log
        absl::log_internal_check_op
        absl::status
        absl::statusor
        absl::strings
        absl::strings_internal
        absl::time
)

install(
        TARGETS actConcurrency
        EXPORT actTargets
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib/act
        ARCHIVE DESTINATION lib/act
)