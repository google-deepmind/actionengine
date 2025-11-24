import asyncio
import os
import random

import actionengine
from redis import asyncio as aioredis


ECHO_SCHEMA = actionengine.ActionSchema(
    name="echo",
    description="An action that echoes input messages to output.",
    inputs=[("input", "text/plain")],
    outputs=[("output", "text/plain")],
)


async def run_echo(action: actionengine.Action):
    action["input"].set_reader_options(timeout=10.0, ordered=True)
    with action["input"].deserialize_automatically():
        async for message in action["input"]:
            await action["output"].put(message)
            print(message)

    worker_name = os.environ.get("WORKER_NAME", "unknown")
    await action["output"].put(f" (worker: {worker_name})")
    print()

    await action["output"].finalize()


def make_action_registry() -> actionengine.ActionRegistry:
    registry = actionengine.ActionRegistry()
    registry.register("echo", ECHO_SCHEMA, run_echo)
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


async def await_action_completion(action: actionengine.Action):
    await action.wait_until_complete()


async def listen_and_execute_actions(
    queue: str,
    action_registry: actionengine.ActionRegistry,
):
    pending_tasks: set[asyncio.Task] = set()
    aio_redis = get_aioredis_client()

    while True:
        _, message = await aio_redis.blpop([queue])
        message = await asyncio.to_thread(
            actionengine.WireMessage.from_msgpack, message
        )

        action_message: actionengine.ActionMessage
        for action_message in message.actions:
            node_map = actionengine.NodeMap(make_redis_chunk_store)
            action = action_registry.make_action(
                action_message.name,
                action_message.id,
                node_map=node_map,
            )
            action.run()

            task = asyncio.create_task(await_action_completion(action))
            pending_tasks.add(task)
            task.add_done_callback(pending_tasks.discard)


async def main():
    action_registry = make_action_registry()
    queue_name = "actionengine:echo"

    execute = asyncio.create_task(
        listen_and_execute_actions(queue_name, action_registry)
    )
    await execute


def sync_main():
    settings = actionengine.get_global_act_settings()
    settings.readers_deserialise_automatically = True
    settings.readers_remove_read_chunks = False

    asyncio.run(main())


if __name__ == "__main__":
    sync_main()
