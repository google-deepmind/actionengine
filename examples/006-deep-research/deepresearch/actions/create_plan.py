import actionengine

from ..gemini import generate_content_stream


async def run(action: actionengine.Action):
    topic = await action["topic"].consume()

    response_parts = []

    prompt = (
        f"You are about to do a research on the following topic: {topic}."
        f"Do not do the research yet. Just think "
        "about your plan and what you will do. You may use Google Search "
        "to make a plan and later do research. Present your "
        "plan clearly step by step, numbering each step, so that they form an "
        "ordered list. Formulate each step as if it was an instruction to "
        "yourself. Do not explain anything yet. Just present your plan. "
        "Start directly with the list, no introduction. The final item of your "
        "plan should start with 'FINALLY: ' and be a detailed instruction "
        "that describes how you will synthesise and present the final result. "
        "Use no more than 4 steps. Make sure that the steps can be "
        "performed independently, because they will be performed by "
        "different agents. Reply in the same language as the input. "
    )

    async for response in await generate_content_stream(
        api_key=await action["api_key"].consume(),
        contents=prompt,
    ):
        for candidate in response.candidates:
            for part in candidate.content.parts:
                if not part.thought:
                    response_parts.append(part)

    full_response = "".join([part.text for part in response_parts])
    lines = full_response.split("\n")
    lines_no_numbers = [" ".join(line.split()[1:]) for line in lines]

    plan_items = action["plan_items"]
    try:
        for line in lines_no_numbers:
            await plan_items.put(line)
    finally:
        await plan_items.finalize()


SCHEMA = actionengine.ActionSchema(
    name="create_plan",
    inputs=[
        ("api_key", "text/plain"),
        ("topic", "text/plain"),
    ],
    outputs=[("plan_items", "text/plain")],
)
