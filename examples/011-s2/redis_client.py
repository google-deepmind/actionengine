import asyncio
import os
import uuid

import actionengine
from redis import asyncio as aioredis


ECHO_SCHEMA = actionengine.ActionSchema(
    name="echo",
    description="An action that echoes input messages to output.",
    inputs=[("input", "text/plain")],
    outputs=[("output", "text/plain")],
)


def make_action_registry() -> actionengine.ActionRegistry:
    registry = actionengine.ActionRegistry()
    registry.register("echo", ECHO_SCHEMA)
    return registry


def get_aioredis_client() -> aioredis.Redis:
    redis_host = os.environ.get("REDIS_HOST", "localhost")
    redis_port = int(os.environ.get("REDIS_PORT", "6379"))
    redis = aioredis.from_url(
        f"redis://{redis_host}:{redis_port}",
        decode_responses=False,
    )
    return redis


def get_actionengine_redis_client():
    if not hasattr(get_actionengine_redis_client, "client"):
        get_actionengine_redis_client.client = actionengine.redis.Redis.connect(
            "localhost"
        )
    return get_actionengine_redis_client.client


def make_redis_chunk_store(node_id: str) -> actionengine.redis.ChunkStore:
    redis_client = get_actionengine_redis_client()
    store = actionengine.redis.ChunkStore(redis_client, node_id, -1)  # No TTL
    return store


async def call_echo(
    message: str,
    action_registry: actionengine.ActionRegistry,
    queue_name: str,
):
    aio_redis = get_aioredis_client()

    node_map = actionengine.NodeMap(make_redis_chunk_store)

    uid = str(uuid.uuid4())
    action = action_registry.make_action(
        "echo",
        action_id=uid,
        node_map=node_map,
    )
    action_message = action_registry.make_action_message("echo", action_id=uid)

    wire_message = actionengine.WireMessage()
    wire_message.actions.append(action_message)

    wire_message_bytes = await asyncio.to_thread(wire_message.pack_msgpack)

    await action["input"].put_and_finalize(message)
    await aio_redis.rpush(queue_name, wire_message_bytes)
    await action.call()

    async for output in action["output"]:
        yield output

    await action.wait_until_complete()

    del node_map
    del action


async def main():
    action_registry = make_action_registry()
    queue_name = "actionengine:echo"

    for i in range(10):
        message = f"Hello, Action Engine! {i + 1}"
        async for response in call_echo(message, action_registry, queue_name):
            print(response, end="")
        print()


def sync_main():
    settings = actionengine.get_global_act_settings()
    settings.readers_deserialise_automatically = True
    settings.readers_remove_read_chunks = True

    asyncio.run(main())


if __name__ == "__main__":
    sync_main()
