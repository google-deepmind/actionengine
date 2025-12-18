from functools import lru_cache

from google import genai
from google.genai import types
from google.genai.types import (
    GenerateContentConfig,
    ThinkingConfig,
    GoogleSearch,
)


@lru_cache(maxsize=1)
def get_gemini_client(api_key: str):
    return genai.client.AsyncClient(
        genai.client.BaseApiClient(
            api_key=api_key,
        )
    )


async def generate_content_stream(
    api_key: str,
    contents: str,
    *,
    model: str = "gemini-2.5-flash",
    config: GenerateContentConfig | None = None,
    system_instruction_override: types.ContentUnion | None = None,
):
    client = get_gemini_client(api_key)

    config = config or GenerateContentConfig(
        thinking_config=ThinkingConfig(
            include_thoughts=True,
            thinking_budget=2048,
        ),
        tools=[types.Tool(google_search=GoogleSearch())],
    )
    if system_instruction_override is not None:
        config.system_instruction = system_instruction_override

    return await client.models.generate_content_stream(
        model=model, contents=contents, config=config
    )
