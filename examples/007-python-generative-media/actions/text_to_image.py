import asyncio

import actionengine
import torch
from diffusers import (
    AutoencoderKL,
    UNet2DConditionModel,
    StableDiffusionPipeline,
    UniPCMultistepScheduler,
)
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
LOCKS = (asyncio.Lock(), asyncio.Lock())
IDX = 0


def get_unet():
    model_candidates = (
        "/opt/durer/data/checkpoints_diffusers/267b356f-7a31-432b-80cd-bc5b3952091f",
        "/home/ubuntu/267b356f-7a31-432b-80cd-bc5b3952091f",
        "stable-diffusion-v1-5/stable-diffusion-v1-5",
    )

    device = "cpu"
    if torch.backends.mps.is_available():
        device = "mps"
    if torch.cuda.is_available():
        device = "cuda"

    for candidate_model_id in model_candidates:
        try:
            unet = UNet2DConditionModel.from_pretrained(
                candidate_model_id,
                subfolder="unet",
                revision=None,
                torch_dtype=torch.float32 if device == "cpu" else torch.float16,
            )
            unet.to(device)
            return unet
        except Exception:
            continue

    raise RuntimeError("UNet not found.")


def get_vae():
    model_id = "stabilityai/sd-vae-ft-ema"
    device = "cpu"
    if torch.backends.mps.is_available():
        device = "mps"
    if torch.cuda.is_available():
        device = "cuda"

    vae = AutoencoderKL.from_pretrained(
        model_id,
        revision=None,
        torch_dtype=torch.float32 if device == "cpu" else torch.float16,
    )
    vae.to(device)
    return vae


def get_pipeline(idx: int = 0) -> StableDiffusionPipeline:
    if not hasattr(get_pipeline, "pipe0"):
        device = "cpu"
        if torch.backends.mps.is_available():
            device = "mps"
        if torch.cuda.is_available():
            device = "cuda"

        get_pipeline.pipe0 = StableDiffusionPipeline.from_pretrained(
            "stable-diffusion-v1-5/stable-diffusion-v1-5",
            torch_dtype=torch.float32 if device == "cpu" else torch.float16,
            unet=get_unet(),
            vae=get_vae(),
            safety_checker=None,
            scheduler=UniPCMultistepScheduler.from_pretrained(
                "stable-diffusion-v1-5/stable-diffusion-v1-5",
                subfolder="scheduler",
                timestep_spacing="trailing",
            ),
            requires_safety_checker=False,
        )

        get_pipeline.pipe0.to(device)

    if not hasattr(get_pipeline, "pipe1"):
        device = "cpu"
        if torch.backends.mps.is_available():
            device = "mps"
        if torch.cuda.is_available():
            device = "cuda"

        get_pipeline.pipe1 = StableDiffusionPipeline.from_pretrained(
            "stable-diffusion-v1-5/stable-diffusion-v1-5",
            torch_dtype=torch.float32 if device == "cpu" else torch.float16,
            unet=get_unet(),
            vae=get_vae(),
            safety_checker=None,
            scheduler=UniPCMultistepScheduler.from_pretrained(
                "stable-diffusion-v1-5/stable-diffusion-v1-5",
                subfolder="scheduler",
            ),
            requires_safety_checker=False,
        )

        get_pipeline.pipe1.to(device)

    if idx == 0:
        return get_pipeline.pipe0
    elif idx == 1:
        return get_pipeline.pipe1

    raise ValueError("Invalid pipeline index. Use 0 or 1.")


def make_progress_callback(action: actionengine.Action):
    def callback(pipeline, step: int, timestep: int, kwargs):
        action["progress"].put(ProgressMessage(step=step))
        return kwargs

    return callback


async def run(action: actionengine.Action):
    request: DiffusionRequest = await action["request"].consume()

    print("Running text_to_image with request:", str(request), flush=True)

    pipe_idx = 0
    async with LOCK:
        global IDX
        pipe_idx = IDX
        pipe = get_pipeline(pipe_idx)
        IDX = (IDX + 1) % 2

    generator = torch.Generator(pipe.device)
    if request.seed is not None:
        generator = generator.manual_seed(request.seed)

    try:
        async with LOCKS[pipe_idx]:
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
