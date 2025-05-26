import asyncio

import evergreen
import torch
from diffusers import StableDiffusionPipeline
from pydantic import BaseModel


class DiffusionRequest(BaseModel):
    prompt: str
    num_inference_steps: int = 20
    height: int = 512
    width: int = 512
    seed: int | None = None


class ProgressMessage(BaseModel):
    step: int


def get_pipeline():
    if not hasattr(get_pipeline, "pipe"):
        get_pipeline.pipe = StableDiffusionPipeline.from_pretrained(
            "stable-diffusion-v1-5/stable-diffusion-v1-5",
            torch_dtype=torch.float16,
            safety_checker=None,
            requires_safety_checker=False,
        )
        get_pipeline.pipe.to("mps")

    return get_pipeline.pipe


def make_progress_callback(action: evergreen.Action):
    def callback(pipeline, step: int, timestep: int, kwargs):
        action["progress"].put(ProgressMessage(step=step))
        return kwargs

    return callback


async def run(action: evergreen.Action):
    request: DiffusionRequest = await action["request"].consume()
    pipe = get_pipeline()

    generator = torch.Generator(pipe.device)
    if request.seed is not None:
        generator = generator.manual_seed(request.seed)

    try:
        images = await asyncio.to_thread(
            pipe,
            request.prompt,
            num_inference_steps=request.num_inference_steps,
            height=request.height,
            width=request.width,
            generator=generator,
            callback_on_step_end=make_progress_callback(action),
        )
    finally:
        await action["progress"].finalize()

    await action["image"].put_and_finalize(images[0][0])


SCHEMA = evergreen.ActionSchema(
    name="text_to_image",
    inputs=[("request", DiffusionRequest)],
    outputs=[("image", "image/png"), ("progress", ProgressMessage)],
)
