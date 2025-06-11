add_library(
        egltMsgpack STATIC
        array.h
        core_helpers.h
        float.h
        misc.h
        msgpack.cc
        int.h
        msgpack.h
        strbin.h
)
add_library(eglt::msgpack ALIAS egltMsgpack)
target_include_directories(
        egltMsgpack PUBLIC
        ${EGLT_PROJECT_TOP_LEVEL}
        ${EGLT_INCLUDE_DIR}
)
target_link_libraries(
        egltMsgpack PUBLIC
        ${EGLT_ABSL_DEPENDENCIES}
)