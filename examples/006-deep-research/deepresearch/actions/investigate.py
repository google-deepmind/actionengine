import actionengine

from ..gemini import generate_content_stream


async def run(action: actionengine.Action):
    topic = await action["topic"].consume()
    brief = await action["brief"].consume()

    prompt = (
        f'As part of a bigger project that researches "{topic}", you are '
        f"given the following brief: {brief}. Please investigate "
        "online sources, making sure to use Google Search, and making necessary "
        "citations. Present your findings in a clear and concise manner, "
        "using bullet points. Make sure to include the sources you used. In the "
        "beginning of your report, concisely mention the brief you were given. "
        "You may, and are encouraged to use Google Search to find relevant "
        "information. Respond in the same language as the input. "
    )

    api_key = await action["api_key"].consume()
    response_parts = []
    async for response in await generate_content_stream(
        api_key=api_key,
        contents=prompt,
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
