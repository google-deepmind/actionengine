import asyncio

import actionengine

from ..gemini import generate_content_stream


SYSTEM_INSTRUCTIONS = [
    "You are a helpful research assistant that helps to synthesise the findings "
    "of a research on a given topic, based on multiple intermediate reports. "
    "You will synthesise the findings into a final report, based on the "
    "intermediate reports you have been given. Make sure to address the brief "
    "you have been given. ",
]


async def run(action: actionengine.Action):
    try:
        topic = await action["topic"].consume()
        brief = await action["brief"].consume()
        report_ids = [report_id async for report_id in action["report_ids"]]

        await action["user_log"].put(
            f"[synthesise_findings] Synthesising findings for topic: {topic}. "
            f"Awaiting reports. "
        )

        node_map = action.get_node_map()
        reports: list[str] = await asyncio.gather(
            *(node_map.get(report_id).consume() for report_id in report_ids)
        )
        await action["user_log"].put(
            f"[synthesise_findings] Synthesising {len(reports)} reports."
        )

        prompt = (
            f"The topic is: {topic}. Report in the same language. You have the "
            f"following {len(reports)} intermediate "
            f"reports: {'\n\n'.join(reports)}\n\n. {brief}"
        )

        async for response in await generate_content_stream(
            api_key=await action["api_key"].consume(),
            contents=prompt,
            system_instruction_override=SYSTEM_INSTRUCTIONS,
        ):
            for candidate in response.candidates:
                for part in candidate.content.parts:
                    if not part.thought:
                        await action["report"].put(part.text)
                    else:
                        await action["thoughts"].put(part.text)

    finally:
        await action["report"].finalize()
        await action["thoughts"].finalize()
        await action["user_log"].put_and_finalize(
            f"[synthesise_findings] Synthesis complete."
        )


SCHEMA = actionengine.ActionSchema(
    name="synthesise_findings",
    inputs=[
        ("api_key", "text/plain"),
        ("topic", "text/plain"),
        ("brief", "text/plain"),
        ("report_ids", "text/plain"),
    ],
    outputs=[
        ("report", "text/plain"),
        ("thoughts", "text/plain"),
        ("user_log", "text/plain"),
    ],
)
