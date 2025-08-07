import asyncio

import actionengine

from stt.actions import make_action_registry
from stt.serialisation import register_stt_serialisers


def setup_action_engine():
    register_stt_serialisers()

    settings = actionengine.get_global_eglt_settings()
    settings.readers_deserialise_automatically = True
    settings.readers_read_in_order = True
    settings.readers_remove_read_chunks = True


async def main():
    setup_action_engine()

    action_registry = make_action_registry()
    service = actionengine.Service(action_registry)
    server = actionengine.websockets.WebsocketServer(service)

    print("Starting Action Engine server.")
    server.run()
    task = asyncio.create_task(asyncio.to_thread(server.join))
    try:
        await task
    except asyncio.CancelledError:
        print("Shutting down Action Engine server.")
        server.cancel()


def sync_main():
    return asyncio.run(main())


if __name__ == "__main__":
    sync_main()
