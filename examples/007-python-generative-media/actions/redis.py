import asyncio

import evergreen
from pydantic import BaseModel


TTL = 120  # Time to live for Redis keys in seconds


def get_eglt_redis_client_for_sub():
    if not hasattr(get_eglt_redis_client_for_sub, "client"):
        get_eglt_redis_client_for_sub.client = evergreen.redis.Redis.connect(
            "localhost"
        )
    return get_eglt_redis_client_for_sub.client


def get_eglt_redis_client_for_pub():
    if not hasattr(get_eglt_redis_client_for_pub, "client"):
        get_eglt_redis_client_for_pub.client = evergreen.redis.Redis.connect(
            "localhost"
        )
    return get_eglt_redis_client_for_pub.client


class ReadRedisStoreRequest(BaseModel):
    key: str
    offset: int = 0
    count: int = 1


def read_store_chunks_into_queue(
    request: ReadRedisStoreRequest,
    queue: asyncio.Queue,
):
    store = evergreen.redis.ChunkStore(
        get_eglt_redis_client_for_sub(), request.key, TTL
    )
    for idx in range(request.offset, request.offset + request.count):
        chunk = store.get(idx)
        queue.put_nowait(chunk)
    queue.put_nowait(None)  # Signal that no more chunks will be added


async def run_read_store(action: evergreen.Action) -> None:
    action.clear_inputs_after_run()

    request: ReadRedisStoreRequest = await action["request"].consume()
    response = action["response"]

    print(
        f"Reading from stream {request.key} with offset {request.offset} and count {request.count}"
    )

    queue = asyncio.Queue(maxsize=request.count + 1)
    asyncio.create_task(
        asyncio.to_thread(read_store_chunks_into_queue, request, queue)
    )

    try:
        while True:
            chunk = await queue.get()
            if chunk is None:
                break

            await response.put(chunk)
    finally:
        await response.finalize()


READ_STORE_SCHEMA = evergreen.ActionSchema(
    name="read_store",
    inputs=[("request", ReadRedisStoreRequest)],
    outputs=[("response", "*")],
)


class WriteRedisStoreRequest(BaseModel):
    class Config:
        arbitrary_types_allowed = True

    key: str
    offset: int = 0
    mimetype: str = "application/octet-stream"
    data: bytes


async def run_write_store(action: evergreen.Action) -> None:
    action.clear_inputs_after_run()

    request: WriteRedisStoreRequest = await action["request"].consume()
    response = action["response"]

    store = evergreen.redis.ChunkStore(
        get_eglt_redis_client_for_pub(), request.key, TTL
    )
    chunk = evergreen.Chunk(
        metadata=evergreen.ChunkMetadata(
            mimetype=request.mimetype,
        ),
        data=request.data,
    )
    store.put(request.offset, chunk, False)
    await response.put_and_finalize("OK")


WRITE_STORE_SCHEMA = evergreen.ActionSchema(
    name="write_store",
    inputs=[("request", WriteRedisStoreRequest)],
    outputs=[("response", "text/plain")],
)
