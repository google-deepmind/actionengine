import asyncio
import io
import uuid

import evergreen
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


class ChunkyChunk(BaseModel):
    node_id: str
    mimetype: str


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


def make_progress_callback(action: evergreen.Action):
    def callback(pipeline, step: int, timestep: int, kwargs):
        action["progress"].put(ProgressMessage(step=step))
        return kwargs

    return callback


async def run(action: evergreen.Action):
    request: DiffusionRequest = await action["request"].consume()

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

    chunky_chunk_uid = str(uuid.uuid4())
    node_map: evergreen.NodeMap = action.get_node_map()
    chunky_node = node_map.get(chunky_chunk_uid)
    chunky_node.bind_stream(action.get_stream())

    bytes_io = io.BytesIO()
    images[0][0].save(bytes_io, format="PNG")
    bytes_io.seek(0)
    image_mimetype = "image/png"
    data = bytes_io.read()
    split_size = 16000  # less than 16kB to avoid fragmentation issues
    split_data = [
        data[i : i + split_size] for i in range(0, len(data), split_size)
    ]
    to_put_splits = [
        (idx, data_piece) for idx, data_piece in enumerate(split_data)
    ]

    await action["image"].put_and_finalize(
        ChunkyChunk(node_id=chunky_chunk_uid, mimetype=image_mimetype)
    )
    tasks = [
        chunky_node.put(data_piece, idx) for idx, data_piece in to_put_splits
    ]
    await asyncio.gather(*tasks)
    await chunky_node.finalize()

    # await action["image"].put_and_finalize(images[0][0])


SCHEMA = evergreen.ActionSchema(
    name="text_to_image",
    inputs=[("request", DiffusionRequest)],
    outputs=[("image", "image/png"), ("progress", ProgressMessage)],
)
