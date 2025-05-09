add_library(
        egltConcurrency STATIC
)
add_library(eglt::concurrency ALIAS egltConcurrency)
target_sources(
        egltConcurrency PRIVATE
        concurrency.cc
)
target_sources(
        egltConcurrency PUBLIC
        concurrency.h
)
target_include_directories(
        egltConcurrency PUBLIC
        ${EGLT_PROJECT_TOP_LEVEL}
        ${EGLT_INCLUDE_DIR}
        ${EGLT_THIRD_PARTY_INCLUDE_DIRS}
)
target_include_directories(
        egltConcurrency PRIVATE
        ${THREAD_ON_BOOST_INCLUDE_DIRS}
)
target_link_libraries(
        egltConcurrency PUBLIC
        ThreadOnBoost
)

install(
        TARGETS egltConcurrency
        EXPORT egltTargets
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib/eglt
        ARCHIVE DESTINATION lib/eglt
)