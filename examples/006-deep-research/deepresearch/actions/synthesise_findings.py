import asyncio

import actionengine

from ..gemini import generate_content_stream


async def run(action: actionengine.Action):
    topic = await action["topic"].consume()
    brief = await action["brief"].consume()
    report_ids = [report_id async for report_id in action["report_ids"]]

    node_map = action.get_node_map()
    reports: list[str] = await asyncio.gather(
        *(node_map.get(report_id).consume() for report_id in report_ids)
    )
    print("All reports have been received.")

    prompt = (
        f"You are asked to synthesise the findings of a research on the "
        f"following topic: {topic}. Report in the same language. You have the "
        f"following {len(reports)} intermediate "
        f"reports: {'\n\n'.join(reports)}\n\n NOW, {brief}"
    )

    try:
        async for response in await generate_content_stream(
            api_key=await action["api_key"].consume(),
            contents=prompt,
        ):
            for candidate in response.candidates:
                for part in candidate.content.parts:
                    if not part.thought:
                        await action["report"].put(part.text)
    finally:
        await action["report"].finalize()


SCHEMA = actionengine.ActionSchema(
    name="synthesise_findings",
    inputs=[
        ("api_key", "text/plain"),
        ("topic", "text/plain"),
        ("brief", "text/plain"),
        ("report_ids", "text/plain"),
    ],
    outputs=[("report", "text/plain")],
)
