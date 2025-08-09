import asyncio
import uuid

from tqdm.auto import tqdm

import actionengine

import actions


def make_action_registry():
    registry = actionengine.ActionRegistry()
    registry.register("echo", actions.echo.SCHEMA)
    registry.register("text_to_image", actions.text_to_image.SCHEMA)
    return registry


async def main():
    action_registry = make_action_registry()
    node_map = actionengine.NodeMap()
    # stream = actionengine.websockets.make_websocket_actionengine_stream(
    #     "localhost", "/", 20000
    # )
    stream = actionengine.webrtc.make_webrtc_stream(
        str(uuid.uuid4()), "demoserver", "demos.helena.direct", 19000
    )

    session = actionengine.Session(node_map, action_registry)
    session.dispatch_from(stream)

    try:
        while True:
            action = action_registry.make_action(
                "text_to_image",
                node_map=node_map,
                stream=stream,
                session=session,
            )

            prompt = await asyncio.to_thread(
                input, "Enter a prompt (or /q to quit): "
            )
            if prompt.lower().startswith("/q"):
                print("Exiting client.")
                break

            await action.call()

            num_inference_steps = 20
            await action["request"].put_and_finalize(
                actions.text_to_image.DiffusionRequest(
                    prompt=prompt, num_inference_steps=num_inference_steps
                )
            )

            with tqdm(total=num_inference_steps + 1) as pbar:
                async for _ in action["progress"]:
                    pbar.update(1)

            image = await action["image"].consume()
            print("Received image:", image)
            image.save("output.png")
    finally:
        session.stop_dispatching_from(stream)
        try:
            del action
        except NameError:
            pass


def setup_action_engine():
    settings = actionengine.get_global_act_settings()
    settings.readers_deserialise_automatically = True
    settings.readers_read_in_order = True
    settings.readers_remove_read_chunks = True

    # a temporary hack to get the schema registered for serialization
    actionengine.to_chunk(actions.text_to_image.ProgressMessage(step=1))


def sync_main():
    setup_action_engine()
    asyncio.run(main())


if __name__ == "__main__":
    sync_main()
