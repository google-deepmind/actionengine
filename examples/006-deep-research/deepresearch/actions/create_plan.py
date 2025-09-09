import asyncio

import actionengine

from ..gemini import generate_content_stream

SYSTEM_INSTRUCTIONS = [
    "You are a helpful research assistant that helps to create a research plan.",
    "You will create a step-by-step plan for researching a given topic.",
    "You may use Google Search to inform your plan.",
    "You will present the plan as an ordered list of steps.",
    "Present your plan clearly step by step, numbering each step, so that they "
    "form an ordered list.",
    "Formulate each step as if it was an instruction to "
    "yourself. Do not explain anything yet. Just present your plan. ",
    "Start directly with the list, no introduction. The final item of your "
    "plan should start with 'FINALLY: ' and be a detailed instruction "
    "that describes how you will synthesise and present the final result. "
    "Use no more than 4 steps. Make sure that the steps can be "
    "performed independently, because they will be performed by "
    "different agents. Reply in the same language as the input. ",
]


async def run(action: actionengine.Action):
    try:
        api_key, topic = await asyncio.gather(
            action["api_key"].consume(),
            action["topic"].consume(),
        )

        response_parts = []

        prompt = f"Here is the research topic: {topic}"

        try:
            await action["user_log"].put(
                f"[create_plan] Creating plan for topic: {topic}."
            )
            async for response in await generate_content_stream(
                api_key=api_key,
                contents=prompt,
                system_instruction_override=SYSTEM_INSTRUCTIONS,
            ):
                for candidate in response.candidates:
                    for part in candidate.content.parts:
                        if not part.thought:
                            response_parts.append(part)
                        else:
                            await action["thoughts"].put(part.text)
        finally:
            await action["thoughts"].finalize()

        await action["user_log"].put("[create_plan] Processing plan items.")

        full_response = "".join([part.text for part in response_parts])
        lines = full_response.split("\n")
        lines_no_numbers = [" ".join(line.split()[1:]) for line in lines]

        plan_items = action["plan_items"]
        try:
            for line in lines_no_numbers:
                await action["user_log"].put(f"[create_plan] Plan item: {line}")
                await plan_items.put(line)
        finally:
            await plan_items.finalize()
    except Exception:
        await action["user_log"].put("[create_plan] Failed to create plan.")
        raise
    else:
        await action["user_log"].put("[create_plan] Finished creating plan.")
    finally:
        await action["user_log"].finalize()


SCHEMA = actionengine.ActionSchema(
    name="create_plan",
    inputs=[
        ("api_key", "text/plain"),
        ("topic", "text/plain"),
    ],
    outputs=[
        ("plan_items", "text/plain"),
        ("thoughts", "text/plain"),
        ("user_log", "text/plain"),
    ],
)
