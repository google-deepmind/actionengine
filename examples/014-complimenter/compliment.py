import argparse
import asyncio
import cv2

import actionengine
from PIL import Image

from lib import actions, utils


def make_ae_instance() -> utils.AEInstance:
    action_registry = actions.make_action_registry()
    print("Action registry created with actions.")

    node_map = actionengine.NodeMap()
    stream = actionengine.websockets.make_websocket_stream(
        "actionengine.dev", "/", 20000
    )

    session = actionengine.Session(node_map, action_registry)
    session.dispatch_from(stream)

    ae = utils.AEInstance(action_registry, session, node_map, stream)
    print("AEInstance created.")
    return ae


async def main(args: argparse.Namespace):
    ae = make_ae_instance()

    cam = cv2.VideoCapture(0)

    print(
        "Press Enter to snap a picture and "
        "get compliments (or type 'exit' to quit): ",
        end="",
    )

    while True:
        prompt = input()
        if prompt.lower() == "exit":
            break

        ret, frame = cam.read()
        if not ret:
            print("Failed to grab frame")
            break

        image = Image.fromarray(cv2.cvtColor(frame, cv2.COLOR_BGR2RGB))
        min_size = min(image.size)
        if min_size > 512:
            scale = 512 / min_size
            new_size = (int(image.size[0] * scale), int(image.size[1] * scale))
            image = image.resize(new_size)

        find_objects = await ae.call("find_objects_in_image")
        await find_objects["image"].put_and_finalize(image)

        found_objects = [obj async for obj in find_objects["objects"]]

        if not found_objects:
            break

        for obj in found_objects:
            print(f" - {obj.name} ({', '.join(obj.attributes)})")

        for obj in found_objects:
            make_compliment = await ae.call("make_compliment")
            await make_compliment["image"].put_and_finalize(image)
            await make_compliment["object"].put_and_finalize(obj)
            compliment = await make_compliment["compliment"].consume()

            print(compliment)
            print()

    ae.stream.half_close()
    cam.release()


def sync_main(args: argparse.Namespace):
    settings = actionengine.get_global_act_settings()
    settings.readers_deserialise_automatically = True
    settings.readers_timeout = 600

    return asyncio.run(main(args))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the VLM4RWD client.")
    args = parser.parse_args()
    sync_main(args)
