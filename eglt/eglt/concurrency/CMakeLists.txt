add_library(
        eglt_concurrency INTERFACE
)
add_library(eglt::concurrency ALIAS eglt_concurrency)
target_precompile_headers(
        eglt_concurrency INTERFACE
        concurrency.h
)
target_include_directories(
        eglt_concurrency INTERFACE
        ${EGLT_PROJECT_TOP_LEVEL}
        ${EGLT_INCLUDE_DIR}
        ${EGLT_THIRD_PARTY_INCLUDE_DIRS}
        ${THREAD_ON_BOOST_INCLUDE_DIRS}
)
target_link_libraries(
        eglt_concurrency INTERFACE
        ThreadOnBoost
)

