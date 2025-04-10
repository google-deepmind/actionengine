add_library(
        egltConcurrency INTERFACE
)
add_library(eglt::concurrency ALIAS egltConcurrency)
target_precompile_headers(
        egltConcurrency INTERFACE
        concurrency.h
)
target_include_directories(
        egltConcurrency INTERFACE
        ${EGLT_PROJECT_TOP_LEVEL}
        ${EGLT_INCLUDE_DIR}
        ${EGLT_THIRD_PARTY_INCLUDE_DIRS}
        ${THREAD_ON_BOOST_INCLUDE_DIRS}
)
target_link_libraries(
        egltConcurrency INTERFACE
        ThreadOnBoost
)

install(
        TARGETS egltConcurrency
        EXPORT egltTargets
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib/eglt
        ARCHIVE DESTINATION lib/eglt
)