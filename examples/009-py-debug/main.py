import argparse
import asyncio
import os
import uuid

import actionengine


def setup_action_engine():
    settings = actionengine.get_global_act_settings()
    settings.readers_deserialise_automatically = True
    settings.readers_read_in_order = True
    settings.readers_remove_read_chunks = True


async def sleep_forever():
    while True:
        await asyncio.sleep(1)


def get_redis_client():
    if not hasattr(get_redis_client, "client"):
        redis_host = os.environ.get("REDIS_HOST", "localhost")
        get_redis_client.client = actionengine.redis.Redis.connect(redis_host)
    return get_redis_client.client


def make_redis_chunk_store(store_id: str) -> actionengine.redis.ChunkStore:
    redis_client = get_redis_client()
    return actionengine.redis.ChunkStore(redis_client, store_id, -1)


async def main(args: argparse.Namespace):
    uid = uuid.uuid4()
    node_id = f"test-node-{uid}"

    print(f"Using {node_id} as node ID.")

    test_node = actionengine.AsyncNode(node_id, make_redis_chunk_store(node_id))
    await test_node.put_and_finalize("hello!")
    print("Put 'hello!' into test node.")
    print(await test_node.consume())
    # action_registry = make_action_registry()
    # service = actionengine.Service(action_registry)
    # # server = actionengine.websockets.WebsocketActionEngineServer(service)
    # rtc_config = actionengine.webrtc.RtcConfig()
    # rtc_config.turn_servers = [
    #     actionengine.webrtc.TurnServer.from_string(
    #         "helena:actionengine-webrtc-testing@actionengine.dev",
    #     ),
    # ]
    # server = actionengine.webrtc.WebRtcServer(
    #     service,
    #     args.host,
    #     args.port,
    #     args.webrtc_signalling_server,
    #     args.webrtc_signalling_port,
    #     args.webrtc_identity,
    #     rtc_config,
    # )
    #
    # server.run()
    # try:
    #     await sleep_forever()
    # except asyncio.CancelledError:
    #     print("Shutting down Action Engine server.")
    #     server.cancel()
    # finally:
    #     await asyncio.to_thread(server.join)


def sync_main(args: argparse.Namespace):
    setup_action_engine()
    asyncio.run(main(args), debug=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    args = parser.parse_args()

    sync_main(args)
