add_executable(redis_playground
        chunk_store.h
        playground.cc
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
target_include_directories(
        redis_playground PRIVATE
        ${EGLT_PROJECT_TOP_LEVEL}
        ${EGLT_INCLUDE_DIR}
        ${EGLT_THIRD_PARTY_INCLUDE_DIRS}
)
target_link_libraries(
        redis_playground PRIVATE
        cppack
        hiredis::hiredis
        eglt::concurrency
        eglt::data
        eglt::msgpack
        eglt::stores
        eglt::util
        G3Fiber
        ${EGLT_ABSL_DEPENDENCIES}
)

#add_library(egltRedis STATIC)
#add_library(eglt::redis ALIAS egltRedis)
#
#target_sources(egltRedis
#        PUBLIC
#        conversion.h
#        eg_structs.h
#        mimetypes.h
#        msgpack.h
#        serialization.h
#)
#target_sources(egltRedis
#        PRIVATE
#        eg_structs.cc
#)
#target_include_directories(
#        egltRedis PUBLIC
#        ${EGLT_PROJECT_TOP_LEVEL}
#        ${EGLT_INCLUDE_DIR}
#        ${EGLT_THIRD_PARTY_INCLUDE_DIRS}
#)
#target_link_libraries(
#        egltRedis PUBLIC
#        cppack
#        eglt::msgpack
#        ${EGLT_ABSL_DEPENDENCIES}
#)
#
#install(
#        TARGETS egltRedis
#        EXPORT egltTargets
#        RUNTIME DESTINATION bin
#        LIBRARY DESTINATION lib/eglt
#        ARCHIVE DESTINATION lib/eglt
#)