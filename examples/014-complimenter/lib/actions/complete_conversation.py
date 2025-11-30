import asyncio
import base64
import io
import threading
from typing import ClassVar

import actionengine
from actionengine.pydantic_helpers import ActSchema
from transformers import AsyncTextIteratorStreamer
from PIL import Image


class Content(ActSchema):
    _act_schema_name: ClassVar[str] = "Content"

    type: str
    image: bytes | None = None
    text: str | None = None


class Message(ActSchema):
    _act_schema_name: ClassVar[str] = "Message"

    role: str
    content: list[Content]


COMPLETE_CONVERSATION_SCHEMA = actionengine.ActionSchema(
    name="complete_conversation",
    inputs=[("conversation", Message)],
    outputs=[("response", "text/plain")],
)


async def run_complete_conversation(action: actionengine.Action):
    # Lazy import so that clients don't need PyTorch
    from ..models import get_vision_instruct_llama

    messages = []
    msg: Message
    images = []
    async for msg in action["conversation"]:
        msg_dict = msg.model_dump(exclude_none=True)
        for i, content in enumerate(msg.content):
            if content.type == "text":
                continue
            if content.image is not None:
                b64_bytes = await asyncio.to_thread(
                    base64.b64encode, content.image
                )
                images.append(b64_bytes.decode("utf-8"))
        messages.append(msg_dict)

    model, processor = await get_vision_instruct_llama()

    input_text = processor.apply_chat_template(
        messages, add_generation_prompt=True
    )
    inputs = processor(
        *images,
        input_text,
        add_special_tokens=False,
        return_tensors="pt",
    ).to(model.device)

    input_length = inputs["input_ids"].shape[1]

    output = model.generate(**inputs, max_new_tokens=512)
    output = processor.decode(
        output[0][input_length:], skip_special_tokens=True
    )
    await action["response"].put_and_finalize(output)


async def run_complete_conversation_qwen(action: actionengine.Action):
    # Lazy import so that clients don't need PyTorch
    from ..models import get_qwen_model_and_processor

    messages = []
    msg: Message
    images = []
    async for msg in action["conversation"]:
        msg_dict = msg.model_dump(exclude_none=True)
        for i, content in enumerate(msg.content):
            if content.type == "text":
                continue
            if content.image is not None:
                buf = io.BytesIO(content.image)
                images.append(Image.open(buf).convert("RGB"))
                msg_dict["content"][i]["image"] = images[-1]

        messages.append(msg_dict)

    model, processor = await get_qwen_model_and_processor()

    # Prepare model input
    inputs = processor.apply_chat_template(
        messages,
        tokenize=True,
        add_generation_prompt=True,
        return_dict=True,
        return_tensors="pt",
    ).to(model.device)

    tokenizer = processor.tokenizer
    streamer = AsyncTextIteratorStreamer(
        tokenizer,
        skip_prompt=True,
        skip_special_tokens=True,
    )

    generate_kwargs = {"max_new_tokens": 12800, "streamer": streamer}
    generate_kwargs.update(inputs)

    thread = threading.Thread(target=model.generate, kwargs=generate_kwargs)
    thread.start()

    buf = ""

    try:
        async for new_text in streamer:
            buf += new_text
    finally:
        await action["response"].put_and_finalize(buf)


def register(
    registry: actionengine.ActionRegistry,
    name: str = None,
    with_handler: bool = True,
):
    # seed serialisers, this won't be needed later
    actionengine.to_bytes(Content(type="text"))
    actionengine.to_bytes(Message(role="user", content=[]))

    registry.register(
        name or COMPLETE_CONVERSATION_SCHEMA.name,
        COMPLETE_CONVERSATION_SCHEMA,
        run_complete_conversation_qwen if with_handler else None,
    )
