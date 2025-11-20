import asyncio
import json
import re
import threading

import actionengine
import torch
from transformers import (
    AsyncTextIteratorStreamer,
    AutoModelForImageTextToText,
    AutoProcessor,
)

MODEL_ID = "Qwen/Qwen3-VL-2B-Instruct"


def get_model_and_processor():
    if not hasattr(get_model_and_processor, "model"):
        get_model_and_processor.model = (
            AutoModelForImageTextToText.from_pretrained(
                MODEL_ID,
                dtype=torch.bfloat16,
                attn_implementation="sdpa",
                device_map="auto",
            )
        )

    if not hasattr(get_model_and_processor, "processor"):
        get_model_and_processor.processor = AutoProcessor.from_pretrained(
            MODEL_ID
        )

    return get_model_and_processor.model, get_model_and_processor.processor


PROMPT = """You are performing OCR.

For the given image, find all paragraphs, headings, or other logical text groupings (not individual lines or characters).  
For each grouping, output one JSON object containing the text and its bounding box coordinates `[x1, y1, x2, y2]`.

Output **only** valid JSON objects — one per line — with no markdown, no extra commentary, and no punctuation after each line.

Each line must be a single JSON object in exactly this format:
{"text": "<extracted text>", "bbox": [x1, y1, x2, y2]}

Rules:
- Do not include any code fences (no ```json or ```).
- Do not include explanations or summaries.
- Do not include extra brackets or arrays — just one JSON object per line.
- Do not wrap the output in quotes.
- Do not start with phrases like "Here is the result".
- Do not number the items.
- Do not produce markdown.

Example (desired format):

{"text": "Main Heading", "bbox": [42, 18, 310, 65]}
{"text": "First paragraph of text here", "bbox": [40, 80, 330, 150]}
{"text": "Another block of text", "bbox": [38, 165, 325, 230]}

Now, output only in that format.
"""


class TextBox(actionengine.pydantic_helpers.ActSchema):
    _act_schema_name = "TextBox"

    text: str
    bbox: tuple[int, int, int, int]


SCHEMA = actionengine.ActionSchema(
    name="ocr",
    inputs=[("image", "image/png")],
    outputs=[("text_boxes", TextBox), ("_user_log", "text/plain")],
    description=(
        f"Extracts text boxes from an image using a vision-language model. "
        f"Outputs each text box as a JSON object with text and bounding box "
        f"coordinates:\n{json.dumps(TextBox.model_json_schema(mode="serialization"))}"
    ),
)


async def run(action: actionengine.Action):
    model, processor = get_model_and_processor()
    user_log = action["_user_log"]

    try:
        await user_log.put("Model and processor loaded.")

        image = await action["image"].consume(timeout=10)
        if max(image.size) > 1024:
            factor = 1024 / max(image.size)
            new_size = (
                int(image.size[0] * factor),
                int(image.size[1] * factor),
            )
            image = image.resize(new_size)
            await user_log.put(f"Image resized to {new_size} for processing.")
        await user_log.put("Image received for OCR.")

        messages = [
            {
                "role": "user",
                "content": [
                    {"type": "image", "image": image},
                    {"type": "text", "text": PROMPT},
                ],
            }
        ]

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

        await user_log.put("Starting OCR extraction through VLM...")
        thread = threading.Thread(target=model.generate, kwargs=generate_kwargs)
        thread.start()

        buffer = ""
        json_pattern = re.compile(r"\{.*?\}(?=\n|$)", re.DOTALL)

        try:
            async for new_text in streamer:
                buffer += new_text

                # Extract all complete JSON objects in the buffer
                matches = list(json_pattern.finditer(buffer))
                for match in matches:
                    json_str = match.group(0).strip().strip("` ")
                    try:
                        data = json.loads(json_str)
                        text_box = TextBox(**data)
                        await action["text_boxes"].put(text_box)
                    except Exception as e:
                        await user_log.put(
                            f"Failed to parse JSON: {json_str[:200]}... Error: {e}"
                        )

                # Keep only unparsed remainder (partial JSON)
                buffer = buffer[matches[-1].end() :] if matches else buffer

            # Final cleanup: attempt to parse any leftover JSON
            buffer = buffer.strip()
            if buffer.startswith("{") and buffer.endswith("}"):
                try:
                    data = json.loads(buffer)
                    text_box = TextBox(**data)
                    await action["text_boxes"].put(text_box)
                except Exception as e:
                    await user_log.put(
                        f"Failed to parse leftover JSON: {buffer[:200]}... Error: {e}"
                    )

        finally:
            await action["text_boxes"].finalize()
            await asyncio.to_thread(thread.join)
            await user_log.put("OCR extraction finished.")
    finally:
        await user_log.finalize()
