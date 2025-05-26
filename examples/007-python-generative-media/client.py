import asyncio
from tqdm.auto import tqdm

import evergreen

import actions


def make_action_registry():
    registry = evergreen.ActionRegistry()
    registry.register("echo", actions.echo.SCHEMA)
    registry.register("text_to_image", actions.text_to_image.SCHEMA)
    return registry


async def main():
    action_registry = make_action_registry()
    node_map = evergreen.NodeMap()
    stream = evergreen.websockets.make_websocket_evergreen_stream(
        "localhost", "/", 20000
    )

    session = evergreen.Session(node_map, action_registry)
    session.dispatch_from(stream)

    try:
        while True:
            action = action_registry.make_action(
                "text_to_image",
                node_map=node_map,
                stream=stream,
                session=session,
            )
            await action.call()

            prompt = await asyncio.to_thread(input)
            if prompt.lower().startswith("/q"):
                print("Exiting client.")
                break

            num_inference_steps = 4
            await action["request"].put_and_finalize(
                actions.text_to_image.DiffusionRequest(
                    prompt=prompt, num_inference_steps=num_inference_steps
                )
            )

            with tqdm(total=num_inference_steps) as pbar:
                async for _ in action["progress"]:
                    pbar.update(1)

            image = await action["image"].consume()
            print(image)
    finally:
        session.stop_dispatching_from(stream)
        try:
            del action
        except NameError:
            pass


def setup_action_engine():
    settings = evergreen.get_global_eglt_settings()
    settings.readers_deserialise_automatically = True
    settings.readers_read_in_order = True
    settings.readers_remove_read_chunks = True

    # a temporary hack to get the schema registered for serialization
    evergreen.to_chunk(actions.text_to_image.ProgressMessage(step=1))


def sync_main():
    setup_action_engine()
    asyncio.run(main())


if __name__ == "__main__":
    sync_main()
