import asyncio
import json
from typing import Annotated, Any

import evergreen
from fastapi import APIRouter, FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from .http import ActionEngineClient, fragment_to_json


TTL = 120  # Time to live for Redis keys in seconds
MAX_READ_TIMEOUT_SECONDS = 300  # Maximum read timeout in seconds


def get_eglt_redis_client_for_sub():
    if not hasattr(get_eglt_redis_client_for_sub, "client"):
        get_eglt_redis_client_for_sub.client = evergreen.redis.Redis.connect(
            "localhost"
        )
    return get_eglt_redis_client_for_sub.client
    # return evergreen.redis.Redis.connect("localhost")


def get_eglt_redis_client_for_pub():
    if not hasattr(get_eglt_redis_client_for_pub, "client"):
        get_eglt_redis_client_for_pub.client = evergreen.redis.Redis.connect(
            "localhost"
        )
    return get_eglt_redis_client_for_pub.client


class ReadStoreRequest(BaseModel):
    key: str = Field(
        ...,
        description="The stream ID to read from. "
        "This is the identifier for the stream and the underlying ChunkStore.",
    )
    offset: int = Field(
        default=0,
        description="The offset in the stream to start reading from. ",
    )
    count: int = Field(
        default=1,
        description="The maximum number of chunks to read from the stream. "
        "If not specified, defaults to 1.",
    )
    timeout: float = Field(
        default=10.0,
        ge=0.0,
        le=MAX_READ_TIMEOUT_SECONDS,
        description="The maximum time to wait for each chunk in seconds. "
        "If set to 0, it means return immediately if the chunk is not "
        f"available. Must be a non-negative value and not "
        f"exceed {MAX_READ_TIMEOUT_SECONDS} seconds.",
    )
    store_uri: str | None = Field(
        default=None,
        description="Optional URI of the store to read from. "
        "If not provided, the server's default shared store is used.",
    )


async def read_store_chunks_into_queue(
    request: ReadStoreRequest,
    queue: asyncio.Queue,
    annotation: str | None = None,
):
    def annotate(value: Any):
        if annotation is None:
            return value
        if isinstance(value, tuple):
            return annotation, *value
        return annotation, value

    redis_client = get_eglt_redis_client_for_sub()
    store = evergreen.redis.ChunkStore(redis_client, request.key, TTL)
    hi = request.offset + request.count if request.count > 0 else 2147483647
    if (final_seq := await asyncio.to_thread(store.get_final_seq)) != -1:
        hi = min(hi, final_seq + 1)
    try:
        for seq in range(request.offset, hi):
            chunk = await asyncio.to_thread(store.get, seq, request.timeout)
            final_seq = await asyncio.to_thread(store.get_final_seq)
            await queue.put(annotate((chunk, seq, seq == final_seq)))
            if seq == final_seq:
                break
    except Exception as exc:
        print(f"Error reading from store {request.key}: {exc}")
        await queue.put(annotate(exc))
    await queue.put(annotate(None))  # Signal that no more chunks will be added


async def read_store_run(action: evergreen.Action) -> None:
    response = action["response"]

    try:
        action.clear_inputs_after_run()

        request: ReadStoreRequest = await action["request"].consume()

        maxsize = request.count + 1 if request.count > 0 else 32
        queue = asyncio.Queue(maxsize=maxsize)
        _ = asyncio.create_task(read_store_chunks_into_queue(request, queue))

        while True:
            element = await queue.get()
            if element is None:
                break

            if isinstance(element, Exception):
                raise element

            chunk, seq, is_final = element
            await response.put(chunk)
    finally:
        await response.finalize()


READ_STORE_SCHEMA = evergreen.ActionSchema(
    name="read_store",
    inputs=[("request", ReadStoreRequest)],
    outputs=[("response", "*")],
)


class WriteRedisStoreRequest(BaseModel):
    class Config:
        arbitrary_types_allowed = True

    key: str
    offset: int = 0
    mimetype: str = "application/octet-stream"
    data: bytes


async def write_store_run(action: evergreen.Action) -> None:
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


def register_actions(
    registry: evergreen.ActionRegistry | None = None,
) -> evergreen.ActionRegistry:
    """
    Create and return an ActionRegistry with the necessary actions registered.
    """
    registry = registry or evergreen.ActionRegistry()
    registry.register(
        "read_store",
        READ_STORE_SCHEMA,
        read_store_run,
    )
    registry.register(
        "write_store",
        WRITE_STORE_SCHEMA,
        write_store_run,
    )
    return registry


async def read_store_http_handler_impl(
    request: ReadStoreRequest,
    stream: bool = False,
):
    """
    Create an action to read from the Redis store.
    """
    # ae = ActionEngineClient.global_instance()
    # action = ae.make_action("read_store")

    # await asyncio.gather(
    #     action.call(),
    #     action["request"].put_and_finalize(request),
    # )

    queue = asyncio.Queue(maxsize=request.count + 1)
    read_task = asyncio.create_task(
        read_store_chunks_into_queue(request, queue)
    )

    try:
        current_mimetype = None
        while True:
            try:
                element = await queue.get()
            except asyncio.CancelledError:
                break
            if element is None:
                break

            if isinstance(element, Exception):
                exc = HTTPException(
                    status_code=500,
                    detail=f"Error reading from store: {str(element)}",
                )
                if not stream:
                    raise exc from element
                else:
                    yield f"data: {json.dumps({'error': str(element)})}\n\n"
                    break

            chunk, seq, is_final = element
            fragment = evergreen.NodeFragment(
                id=request.key,
                seq=seq,
                chunk=chunk,
                continued=not is_final,
            )
            include_metadata = (
                current_mimetype != fragment.chunk.metadata.mimetype
            )
            current_mimetype = fragment.chunk.metadata.mimetype
            if stream:
                yield f"data: {json.dumps(fragment_to_json(fragment, include_metadata=include_metadata))}\n\n"
            else:
                yield fragment
    finally:
        read_task.cancel()
        try:
            await read_task
        except asyncio.CancelledError:
            pass


async def read_store_http_handler(
    key: str,
    offset: int = 0,
    count: int = 1,
    stream: bool = False,
    timeout: float = 10.0,
):
    """
    Read from a ChunkStore and return/stream the chunks. Starts reading from
    the specified offset and reads up to the specified count of chunks.
    Timeout is in seconds, maximum is MAX_READ_TIMEOUT_SECONDS seconds,
    and it is applied to reading each chunk, not the entire operation.

    If a timeout occurs while reading a chunk, an error event will be sent
    in streaming mode. In non-streaming mode, if at least one chunk was read
    before the timeout, those chunks will be returned. If no chunks were read,
    an HTTP 500 error will be raised with the timeout error message.
    """

    if timeout < 0:
        raise HTTPException(
            status_code=400,
            detail="Timeout must be a non-negative value.",
        )
    if timeout > MAX_READ_TIMEOUT_SECONDS:
        raise HTTPException(
            status_code=400,
            detail=f"Timeout must not exceed {MAX_READ_TIMEOUT_SECONDS} seconds.",
        )

    request = ReadStoreRequest(
        key=key,
        offset=offset,
        count=count,
        timeout=timeout,
    )

    if stream:
        return StreamingResponse(
            read_store_http_handler_impl(request, stream=True),
            media_type="text/event-stream",
        )

    fragments = []
    try:
        async for fragment in read_store_http_handler_impl(
            request, stream=False
        ):
            fragments.append(fragment)
    except HTTPException as exc:
        if not fragments:
            exc.status_code = 504
            raise exc

    current_mimetype = None
    fragment_jsons = []
    for fragment in fragments:
        include_metadata = current_mimetype != fragment.chunk.metadata.mimetype
        current_mimetype = fragment.chunk.metadata.mimetype
        fragment_jsons.append(
            fragment_to_json(fragment, include_metadata=include_metadata)
        )
    return fragment_jsons


def register_http_routes(
    app: FastAPI,
) -> FastAPI:
    """
    Register HTTP routes for the actions in the ActionRegistry.
    """

    streams = APIRouter(prefix="/streams", tags=["streams"])
    streams.get(
        "/read",
        summary="Read from a chunk store",
        response_description="List or stream of NodeFragments",
        responses={
            200: {
                "description": "List of NodeFragments or a stream of NodeFragments",
                "content": {
                    "application/json": {
                        "example": [
                            {
                                "id": "stream_id",
                                "seq": 0,
                                "chunk": {
                                    "metadata": {"mimetype": "text/plain"},
                                    "data": "Hello, world!",
                                },
                                "continued": False,
                            }
                        ]
                    },
                    "text/event-stream": {
                        "example": 'data: {"id": "stream_id", "seq": 0, "chunk": {"metadata": {"mimetype": "text/plain"}, "data": "Hello, world!"}, "continued": false}\n\n'
                    },
                },
            },
            400: {"description": "Invalid request parameters"},
            500: {"description": "Error reading from store"},
        },
    )(read_store_http_handler)
    app.include_router(streams)

    return app
