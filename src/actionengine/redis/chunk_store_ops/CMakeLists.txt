add_library(actRedisChunkStoreOps STATIC
        close_writes.lua.h
        pop.lua.h
        put.lua.h
        unindent.cc
        unindent.h
)

add_library(act::redis::chunk_store_ops ALIAS actRedisChunkStoreOps)

target_include_directories(
        actRedisChunkStoreOps PRIVATE
        ${ACTIONENGINE_PROJECT_TOP_LEVEL}
        ${ACTIONENGINE_INCLUDE_DIR}
        ${ACTIONENGINE_THIRD_PARTY_INCLUDE_DIRS}
)