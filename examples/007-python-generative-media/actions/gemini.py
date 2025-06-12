import os
import traceback

import evergreen
from google import genai
from google.genai import types


def get_gemini_client(api_key: str):
    return genai.client.AsyncClient(
        genai.client.BaseApiClient(
            api_key=api_key,
        )
    )


async def generate_content(action: evergreen.Action):
    api_key = await action["api_key"].consume()

    if not api_key:
        await action["output"].put_and_finalize("API key is required.")
        return

    if api_key == "THISISASECRET :)":
        api_key = os.environ.get("GEMINI_API_KEY")

    client = get_gemini_client(api_key)
    prompt = await action["prompt"].consume()

    retries_left = 3
    try:
        while retries_left > 0:
            try:
                stream = await client.models.generate_content_stream(
                    model="gemini-2.5-flash-preview-05-20",
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        thinking_config=types.ThinkingConfig(
                            include_thoughts=True,
                            thinking_budget=2048,
                        ),
                        tools=[types.Tool(google_search=types.GoogleSearch())],
                    ),
                )
                async for chunk in stream:
                    for candidate in chunk.candidates:
                        if not candidate.content:
                            continue
                        if not candidate.content.parts:
                            continue

                        for part in candidate.content.parts:
                            if not part.thought:
                                await action["output"].put(part.text)
                            else:
                                await action["thoughts"].put(part.text)
                break
            except Exception:
                retries_left -= 1
                await action["output"].put(
                    f"Retrying due to an internal error... {retries_left} retries left."
                )
                if retries_left == 0:
                    await action["output"].put(
                        "Failed to connect to Gemini API."
                    )
                    traceback.print_exc()
                    return
    except Exception:
        traceback.print_exc()
    finally:
        await action["output"].finalize()
        await action["thoughts"].finalize()


GENERATE_CONTENT_SCHEMA = evergreen.ActionSchema(
    name="generate_content",
    inputs=[
        ("api_key", "text/plain"),
        ("prompt", "text/plain"),
    ],
    outputs=[
        ("output", "text/plain"),
        ("thoughts", "text/plain"),
    ],
)
