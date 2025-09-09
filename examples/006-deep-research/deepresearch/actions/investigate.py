import asyncio

import actionengine

from ..gemini import generate_content_stream

SYSTEM_INSTRUCTIONS = [
    "You are a helpful research assistant that helps to investigate a given brief "
    "in the context of a broader research topic.",
    "You will investigate the brief, making sure to use Google Search, and making "
    "necessary citations.",
    "Present your findings in a clear and concise manner, using bullet points.",
    "Make sure to include the sources you used.",
    "In the beginning of your report, concisely mention the brief you were given.",
    "Respond in the same language as the input.",
]


async def run(action: actionengine.Action):
    api_key, topic, brief = await asyncio.gather(
        action["api_key"].consume(),
        action["topic"].consume(),
        action["brief"].consume(),
    )

    prompt = (
        f'The research topic is "{topic}".\n'
        f"The brief for your investigation is as follows: {brief}\n\n"
    )

    response_parts = []
    async for response in await generate_content_stream(
        api_key=api_key,
        contents=prompt,
        system_instruction_override=SYSTEM_INSTRUCTIONS,
    ):
        for candidate in response.candidates:
            for part in candidate.content.parts:
                if not part.thought:
                    response_parts.append(part)

    await action["report"].put_and_finalize(
        "".join([part.text for part in response_parts])
    )


SCHEMA = actionengine.ActionSchema(
    name="investigate",
    inputs=[
        ("api_key", "text/plain"),
        ("topic", "text/plain"),
        ("brief", "text/plain"),
    ],
    outputs=[("report", "text/plain")],
)
