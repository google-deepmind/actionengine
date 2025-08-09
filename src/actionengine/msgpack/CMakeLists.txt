add_library(
        actMsgpack STATIC
        array.h
        core_helpers.h
        float.h
        misc.h
        msgpack.cc
        int.h
        msgpack.h
        strbin.cc
        strbin.h
)
add_library(act::msgpack ALIAS actMsgpack)
target_include_directories(
        actMsgpack PUBLIC
        ${ACTIONENGINE_PROJECT_TOP_LEVEL}
        ${ACTIONENGINE_INCLUDE_DIR}
)
target_link_libraries(
        actMsgpack PRIVATE
        absl::inlined_vector
)