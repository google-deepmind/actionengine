add_subdirectory(chunk_store_ops)

add_library(egltRedis STATIC
        chunk_store.cc
        chunk_store.h
        pubsub.cc
        pubsub.h
        redis.cc
        redis.h
        reply.cc
        reply.h
        reply_converters.cc
        reply_converters.h
        reply_parsers.cc
        reply_parsers.h
        streams.cc
        streams.h
)
add_library(eglt::redis ALIAS egltRedis)
target_include_directories(
        egltRedis PRIVATE
        ${EGLT_PROJECT_TOP_LEVEL}
        ${EGLT_INCLUDE_DIR}
        ${EGLT_THIRD_PARTY_INCLUDE_DIRS}
)
target_link_libraries(
        egltRedis PRIVATE
        cppack
        hiredis::hiredis
        eglt::concurrency
        eglt::data
        eglt::msgpack
        eglt::redis::chunk_store_ops
        eglt::stores
        eglt::util
        G3Fiber
        libuv::uv
        uvw::uvw
)

add_executable(redis_playground
        playground.cc
)
target_link_libraries(
        redis_playground
        absl::failure_signal_handler
        eglt::redis
        eglt::data
        eglt::nodes
        eglt::util
)
target_include_directories(
        redis_playground PRIVATE
        ${EGLT_PROJECT_TOP_LEVEL}
        ${EGLT_INCLUDE_DIR}
        ${EGLT_THIRD_PARTY_INCLUDE_DIRS}
)

# ------------------------------------------------------------------

add_library(
        egltRedisPybind11 STATIC
)
add_library(
        eglt::redis::pybind11 ALIAS
        egltRedisPybind11
)
target_sources(
        egltRedisPybind11 PRIVATE
        chunk_store_pybind11.cc
)
target_sources(
        egltRedisPybind11 PUBLIC
        chunk_store_pybind11.h
)
target_link_libraries(
        egltRedisPybind11 PRIVATE
        eglt::concurrency
        eglt::redis
        cppack
        ${EGLT_PYBIND11_DEPENDENCIES}
)
target_include_directories(
        egltRedisPybind11 PUBLIC
        ${EGLT_INCLUDE_DIRS}
        ${EGLT_PROJECT_TOP_LEVEL}
        ${EGLT_INCLUDE_DIR}
        ${EGLT_THIRD_PARTY_INCLUDE_DIRS}
)

install(
        TARGETS egltRedis
        EXPORT egltTargets
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib/eglt
        ARCHIVE DESTINATION lib/eglt
)
