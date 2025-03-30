add_library(
    eglt_concurrency_boost_fiber INTERFACE
)
target_precompile_headers(
    eglt_concurrency_boost_fiber INTERFACE
    boost_fiber.h
)
target_link_libraries(
    eglt_concurrency_boost_fiber INTERFACE
    eglt_absl_headers
)

add_library(
    eglt_concurrency INTERFACE
)
target_precompile_headers(
    eglt_concurrency INTERFACE
    concurrency.h
)
target_compile_definitions(
    eglt_concurrency INTERFACE
    __EGLT_CONCURRENCY_IMPLEMENTATION__
    __EGLT_CONCURRENCY_IMPLEMENTATION_BOOST_FIBER__
)
target_include_directories(
    eglt_concurrency INTERFACE
    ${EGLT_PROJECT_TOP_LEVEL}
    ${EGLT_INCLUDE_DIRS}
)
target_link_libraries(
    eglt_concurrency INTERFACE
    eglt_absl_headers
)

