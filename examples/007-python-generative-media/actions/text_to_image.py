import asyncio

import actionengine
import torch
from diffusers import StableDiffusionPipeline, UniPCMultistepScheduler
from pydantic import BaseModel


class DiffusionRequest(BaseModel):
    prompt: str
    num_inference_steps: int = 25
    height: int = 512
    width: int = 512
    seed: int | None = None


class ProgressMessage(BaseModel):
    step: int


LOCK = asyncio.Lock()


def get_pipeline():
    if not hasattr(get_pipeline, "pipe"):
        device = "cpu"
        if torch.backends.mps.is_available():
            device = "mps"
        if torch.cuda.is_available():
            device = "cuda"

        get_pipeline.pipe = StableDiffusionPipeline.from_pretrained(
            "stabilityai/stable-diffusion-2-1",
            torch_dtype=torch.float32 if device == "cpu" else torch.float16,
            safety_checker=None,
            scheduler=UniPCMultistepScheduler.from_pretrained(
                "stabilityai/stable-diffusion-2-1",
                subfolder="scheduler",
            ),
            requires_safety_checker=False,
        )

        get_pipeline.pipe.to(device)

    return get_pipeline.pipe


def make_progress_callback(action: actionengine.Action):
    def callback(pipeline, step: int, timestep: int, kwargs):
        action["progress"].put(ProgressMessage(step=step))
        return kwargs

    return callback


async def run(action: actionengine.Action):
    request: DiffusionRequest = await action["request"].consume()

    print("Running text_to_image with request:", str(request), flush=True)

    async with LOCK:
        pipe = get_pipeline()

    generator = torch.Generator(pipe.device)
    if request.seed is not None:
        generator = generator.manual_seed(request.seed)

    try:
        async with LOCK:
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


SCHEMA = actionengine.ActionSchema(
    name="text_to_image",
    inputs=[("request", DiffusionRequest)],
    outputs=[("image", "image/png"), ("progress", ProgressMessage)],
)
