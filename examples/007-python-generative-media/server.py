import asyncio

import evergreen

import actions


def make_action_registry():
    registry = evergreen.ActionRegistry()

    registry.register("echo", actions.echo.SCHEMA, actions.echo.run)
    registry.register(
        "text_to_image", actions.text_to_image.SCHEMA, actions.text_to_image.run
    )

    return registry


def setup_action_engine():
    settings = evergreen.get_global_eglt_settings()
    settings.readers_deserialise_automatically = True
    settings.readers_read_in_order = True
    settings.readers_remove_read_chunks = True

    # will not be needed later:
    evergreen.to_chunk(
        actions.text_to_image.DiffusionRequest(
            prompt="a hack to get the schema registered for serialization",
        )
    )


async def sleep_forever():
    while True:
        await asyncio.sleep(1)


async def main():
    action_registry = make_action_registry()
    service = evergreen.Service(action_registry)
    server = evergreen.websockets.WebsocketEvergreenServer(service)

    server.run()
    task = asyncio.create_task(asyncio.to_thread(server.join))
    try:
        await sleep_forever()
    except asyncio.CancelledError:
        print("Shutting down Action Engine server.")
        server.cancel()
    finally:
        await task


def sync_main():
    setup_action_engine()
    asyncio.run(main())


if __name__ == "__main__":
    sync_main()
