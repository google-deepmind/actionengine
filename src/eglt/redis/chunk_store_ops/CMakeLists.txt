add_library(egltRedisChunkStoreOps STATIC
        close_writes.lua.h
        pop.lua.h
        put.lua.h
        unindent.cc
        unindent.h
)

add_library(eglt::redis::chunk_store_ops ALIAS egltRedisChunkStoreOps)

target_include_directories(
        egltRedisChunkStoreOps PRIVATE
        ${EGLT_PROJECT_TOP_LEVEL}
        ${EGLT_INCLUDE_DIR}
        ${EGLT_THIRD_PARTY_INCLUDE_DIRS}
)