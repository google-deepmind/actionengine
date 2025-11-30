import asyncio

import torch

from transformers import MllamaForConditionalGeneration, AutoProcessor
from transformers import AutoModelForImageTextToText


LLAMA_VISION_INSTRUCT_MODEL_ID = "meta-llama/Llama-3.2-11B-Vision-Instruct"
QWEN_VISION_INSTRUCT_MODEL_ID = "Qwen/Qwen3-VL-2B-Instruct"


async def get_vision_instruct_llama():
    if not hasattr(get_vision_instruct_llama, "parts"):
        model = await asyncio.to_thread(
            MllamaForConditionalGeneration.from_pretrained,
            LLAMA_VISION_INSTRUCT_MODEL_ID,
            torch_dtype=torch.bfloat16,
            device_map="auto",
        )
        processor = await asyncio.to_thread(
            AutoProcessor.from_pretrained, LLAMA_VISION_INSTRUCT_MODEL_ID
        )
        get_vision_instruct_llama.parts = (model, processor)
    return get_vision_instruct_llama.parts


async def get_qwen_model_and_processor():
    if not hasattr(get_qwen_model_and_processor, "model"):
        get_qwen_model_and_processor.model = await asyncio.to_thread(
            AutoModelForImageTextToText.from_pretrained,
            QWEN_VISION_INSTRUCT_MODEL_ID,
            dtype=torch.bfloat16,
            attn_implementation="sdpa",
            device_map="auto",
        )

    if not hasattr(get_qwen_model_and_processor, "processor"):
        get_qwen_model_and_processor.processor = AutoProcessor.from_pretrained(
            QWEN_VISION_INSTRUCT_MODEL_ID
        )

    return (
        get_qwen_model_and_processor.model,
        get_qwen_model_and_processor.processor,
    )
