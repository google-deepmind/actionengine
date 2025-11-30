import time
from typing import ClassVar

import actionengine
import json
from actionengine.pydantic_helpers import ActSchema
from PIL import Image

from .complete_conversation import Content, Message
from ..utils import image_to_bytes


STRUCTURED_DESCRIPTION_PROMPT = """For any image in the user's messages, provide a comprehensive description of objects (and/or subjects) in the image, in the following JSON format:
{
  "objects": [
    {
      "name": "object_name_1", 
      "attributes": ["attribute1", "attribute2"]
    }, 
    {
      "name": "object_name_2", 
      "attributes": ["attribute1"]
    }
  ]}
Ensure that the JSON is properly formatted. If there are multiple images, provide separate descriptions for each image on separate lines.
Only respond with one JSON per image, without any additional text. Limit yourself to just 3 most salient objects and up to 3 attributes per object.
Respond with JSON only, no explanations, introductions, or conclusions, and no punctuation. Do not leave trailing commas.
"""


class ObjectInImage(ActSchema):
    _act_schema_name: ClassVar[str] = "ObjectInImage"

    name: str
    attributes: list[str]


class ImageDescription(ActSchema):
    _act_schema_name: ClassVar[str] = "ImageDescription"

    objects: list[ObjectInImage]


FIND_OBJECTS_SCHEMA = actionengine.ActionSchema(
    name="find_objects_in_image",
    inputs=[("image", "image/png")],
    outputs=[("objects", ObjectInImage)],
)


async def run_find_objects_in_image(action: actionengine.Action):
    print("find_objects_in_image action started")
    start = time.perf_counter()

    image: Image.Image = await action["image"].consume(timeout=20)
    print(f"  +{time.perf_counter() - start:.2f}s - image consumed")

    print("  starting complete_conversation action")
    complete_conversation = (
        action.get_registry()
        .make_action(
            "complete_conversation",
            node_map=action.get_node_map(),
        )
        .run()
    )
    await complete_conversation["conversation"].put(
        Message(
            role="system",
            content=[Content(type="text", text=STRUCTURED_DESCRIPTION_PROMPT)],
        )
    )
    await complete_conversation["conversation"].put_and_finalize(
        Message(
            role="user",
            content=[Content(type="image", image=await image_to_bytes(image))],
        )
    )
    print(
        f"  +{time.perf_counter() - start:.2f}s - message queued to complete_conversation"
    )

    raw_response = await complete_conversation["response"].consume(timeout=30)
    raw_response = raw_response.replace("\\n", "\n")
    if raw_response.endswith("]"):
        raw_response = f"{raw_response}}}"
    raw_response = raw_response.strip(".").replace("'", '"')
    print(
        f"  +{time.perf_counter() - start:.2f}s - response received from complete_conversation"
    )
    print("  parsing response: ", raw_response)

    try:
        response = json.loads(raw_response)
        description = ImageDescription.model_validate(response)

        for obj in description.objects:
            await action["objects"].put(obj)
    finally:
        await action["objects"].finalize()


def register(
    registry: actionengine.ActionRegistry,
    name: str = None,
    with_handler: bool = True,
):
    # seed serialisers, this won't be needed later
    actionengine.to_bytes(ObjectInImage(name="", attributes=[]))
    actionengine.to_bytes(ImageDescription(objects=[]))

    registry.register(
        name or FIND_OBJECTS_SCHEMA.name,
        FIND_OBJECTS_SCHEMA,
        run_find_objects_in_image if with_handler else None,
    )
