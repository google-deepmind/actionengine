import asyncio

import actionengine
from lang_sam import LangSAM
from PIL import Image


def get_lang_sam() -> LangSAM:
    if not hasattr(get_lang_sam, "model"):
        get_lang_sam.model = LangSAM()
    return get_lang_sam.model


async def locate_objects(action: actionengine.Action):
    try:
        model = get_lang_sam()

        image = await action["image"].consume(timeout=5.0)
        prompt = await action["prompt"].consume(timeout=5.0)

        results = await asyncio.to_thread(model.predict, [image], [prompt])
        for mask in results[0]["masks"]:
            pil_mask = Image.fromarray(mask.astype("uint8") * 255)
            await action["masks"].put(pil_mask)
    finally:
        await action["masks"].finalize()


LOCATE_OBJECTS_SCHEMA = actionengine.ActionSchema(
    name="locate_objects",
    inputs=[("image", "image/png"), ("prompt", "text/plain")],
    outputs=[("masks", "image/png")],
)
