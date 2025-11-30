import time

import actionengine
from PIL import Image

from .complete_conversation import Content, Message
from .find_objects_in_image import ObjectInImage
from ..utils import image_to_bytes


COMPLIMENT_PROMPT = """You will be given a message with an image and an structured object description in the form of:
{
  "name": "object_name",
  "attributes": ["attribute1", "attribute2", ...],
}
Respond with a heartfelt compliment about that object (or subject). Be cheerful,
and respond in first person. Do not add any introductions or conclusions—just the compliment.
Make it stritly one short sentence.
"""


MAKE_COMPLIMENT_SCHEMA = actionengine.ActionSchema(
    name="make_compliment",
    inputs=[("image", "image/png"), ("object", ObjectInImage)],
    outputs=[("compliment", "text/plain")],
)


async def run_make_compliment(action: actionengine.Action):
    print("make_compliment action started")
    start = time.perf_counter()

    object_to_compliment: ObjectInImage = await action["object"].consume(
        timeout=10
    )
    print(f"  +{time.perf_counter() - start:.2f}s - object consumed")

    image: Image.Image = await action["image"].consume(timeout=10)
    print(f"  +{time.perf_counter() - start:.2f}s - image consumed")

    print(f"  object to compliment: {object_to_compliment}")

    complete = (
        action.get_registry()
        .make_action(
            "complete_conversation",
            node_map=action.get_node_map(),
        )
        .run()
    )
    print(
        f"  +{time.perf_counter() - start:.2f}s - complete_conversation action started"
    )

    try:
        await complete["conversation"].put(
            Message(
                role="system",
                content=[Content(type="text", text=COMPLIMENT_PROMPT)],
            )
        )
        await complete["conversation"].put(
            Message(
                role="user",
                content=[
                    Content(type="image", image=await image_to_bytes(image))
                ],
            )
        )
        await complete["conversation"].put(
            Message(
                role="user",
                content=[
                    Content(
                        type="text",
                        text=object_to_compliment.model_dump_json(),
                    )
                ],
            )
        )
    finally:
        await complete["conversation"].finalize()
    print(
        f"  +{time.perf_counter() - start:.2f}s - messages queued to complete_conversation"
    )

    try:
        compliment = await complete["response"].consume(timeout=30)
        print(
            f"  +{time.perf_counter() - start:.2f}s - response received from complete_conversation"
        )
        print(f"  compliment: {compliment}")
        await action["compliment"].put(compliment)
    finally:
        await action["compliment"].finalize()


def register(
    registry: actionengine.ActionRegistry,
    name: str = None,
    with_handler: bool = True,
):
    registry.register(
        name or MAKE_COMPLIMENT_SCHEMA.name,
        MAKE_COMPLIMENT_SCHEMA,
        run_make_compliment if with_handler else None,
    )
