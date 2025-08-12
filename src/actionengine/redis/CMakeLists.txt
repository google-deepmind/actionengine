add_library(actRedis STATIC
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
add_library(act::redis ALIAS actRedis)
target_include_directories(
        actRedis PRIVATE
        ${ACTIONENGINE_PROJECT_TOP_LEVEL}
        ${ACTIONENGINE_INCLUDE_DIR}
        ${ACTIONENGINE_THIRD_PARTY_INCLUDE_DIRS}
)
target_link_libraries(
        actRedis PRIVATE
        cppack
        hiredis::hiredis
        act::concurrency
        act::data
        act::msgpack
        act::stores
        act::util
        Thread
        libuv::uv
        uvw::uvw
)

# ------------------------------------------------------------------

add_library(
        actRedisPybind11 STATIC
)
add_library(
        act::redis::pybind11 ALIAS
        actRedisPybind11
)
target_sources(
        actRedisPybind11 PRIVATE
        chunk_store_pybind11.cc
)
target_sources(
        actRedisPybind11 PUBLIC
        chunk_store_pybind11.h
)
target_link_libraries(
        actRedisPybind11 PRIVATE
        act::concurrency
        act::redis
        cppack
        ${ACTIONENGINE_PYBIND11_DEPENDENCIES}
)
target_include_directories(
        actRedisPybind11 PUBLIC
        ${ACTIONENGINE_INCLUDE_DIRS}
        ${ACTIONENGINE_PROJECT_TOP_LEVEL}
        ${ACTIONENGINE_INCLUDE_DIR}
        ${ACTIONENGINE_THIRD_PARTY_INCLUDE_DIRS}
)

install(
        TARGETS actRedis
        EXPORT actTargets
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib/act
        ARCHIVE DESTINATION lib/act
)
