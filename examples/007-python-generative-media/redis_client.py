import asyncio
import uuid

import evergreen
import evergreen.data

import actions


def make_action_registry():
    registry = evergreen.ActionRegistry()
    registry.register(
        "read_store",
        actions.redis.READ_STORE_SCHEMA,
        actions.redis.read_store_run,
    )
    registry.register(
        "write_store",
        actions.redis.WRITE_STORE_SCHEMA,
        actions.redis.write_store_run,
    )
    return registry


ACTION_REGISTRY: evergreen.ActionRegistry | None = None
NODE_MAP: evergreen.NodeMap | None = None


async def write_text_chunk(
    key: str,
    offset: int,
    text: str = "Hello, world!",
) -> None:
    write_action = ACTION_REGISTRY.make_action(
        "write_store",
        node_map=NODE_MAP,
        stream=None,  # No stream needed for this action
    )
    _ = write_action.run()
    await write_action["request"].put_and_finalize(
        actions.redis.WriteRedisStoreRequest(
            key=key,
            offset=offset,
            mimetype="text/plain",
            data=text.encode("utf-8"),
        )
    )
    await write_action["response"].consume()


async def read_text_chunks(
    key: str,
    offset: int = 0,
    count: int = 1,
):
    read_action = ACTION_REGISTRY.make_action(
        "read_store",
        node_map=NODE_MAP,
        stream=None,  # No stream needed for this action
    )
    _ = read_action.run()
    await read_action["request"].put_and_finalize(
        actions.redis.ReadStoreRequest(
            key=key,
            offset=offset,
            count=count,
        )
    )

    async for text in read_action["response"]:
        yield text


def get_global_redis_client():
    if not hasattr(get_global_redis_client, "client"):
        get_global_redis_client.client = evergreen.redis.Redis.connect(
            "localhost"
        )
    return get_global_redis_client.client


def make_redis_chunk_store(node_id: str) -> evergreen.redis.ChunkStore:
    redis_client = get_global_redis_client()
    store = evergreen.redis.ChunkStore(redis_client, node_id, -1)  # No TTL
    return store


async def print_hello_async():
    print("Hello from the Redis client example!", flush=True)


async def main():
    global ACTION_REGISTRY, NODE_MAP

    ACTION_REGISTRY = make_action_registry()
    NODE_MAP = evergreen.NodeMap()

    key = f"hello-{uuid.uuid4()}"
    num_chunks = 10

    for i in range(num_chunks):
        text = f"Hello, world! {i + 1}"
        print(f"Writing chunk {i + 1}:", text)
        await write_text_chunk(key, i, text)

    idx = 0
    async for text in read_text_chunks(key, offset=0, count=num_chunks):
        print(f"Read text chunk {idx + 1}:", text)
        idx += 1


def setup_action_engine():
    settings = evergreen.get_global_eglt_settings()
    settings.readers_deserialise_automatically = True
    settings.readers_read_in_order = True
    settings.readers_remove_read_chunks = True


def sync_main():
    setup_action_engine()
    asyncio.run(main())


if __name__ == "__main__":
    sync_main()
