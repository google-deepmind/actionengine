import asyncio

import actionengine
from google.genai import types
from google.genai.types import (
    GenerateContentConfig,
    ThinkingConfig,
    GoogleSearch,
)

from .gemini import get_gemini_client


async def create_plan(action: actionengine.Action):
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

    api_key = await action["api_key"].consume()
    async for response in await get_gemini_client(
        api_key
    ).models.generate_content_stream(
        model="gemini-2.5-flash-preview-05-20",
        contents=prompt,
        config=GenerateContentConfig(
            thinking_config=ThinkingConfig(
                include_thoughts=True,
                thinking_budget=2048,
            ),
            tools=[types.Tool(google_search=GoogleSearch())],
        ),
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
            await asyncio.sleep(0.3)  # Simulate some delay
    finally:
        await plan_items.finalize()


CREATE_PLAN_SCHEMA = actionengine.ActionSchema(
    name="create_plan",
    inputs=[
        ("api_key", "text/plain"),
        ("topic", "text/plain"),
    ],
    outputs=[("plan_items", "text/plain")],
)


async def investigate_plan_item(action: actionengine.Action):
    topic = await action["topic"].consume()
    brief = await action["brief"].consume()

    print(f"[debug] Investigating: {action["topic"].get_id()}")

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
    async for response in await get_gemini_client(
        api_key
    ).models.generate_content_stream(
        model="gemini-2.5-flash-preview-05-20",
        contents=prompt,
        config=GenerateContentConfig(
            thinking_config=ThinkingConfig(
                include_thoughts=True,
                thinking_budget=2048,
            ),
            tools=[types.Tool(google_search=GoogleSearch())],
        ),
    ):
        for candidate in response.candidates:
            for part in candidate.content.parts:
                if not part.thought:
                    response_parts.append(part)

    await action.get_output("report").put_and_finalize(
        "".join([part.text for part in response_parts])
    )


INVESTIGATE_PLAN_ITEM_SCHEMA = actionengine.ActionSchema(
    name="investigate_plan_item",
    inputs=[
        ("api_key", "text/plain"),
        ("topic", "text/plain"),
        ("brief", "text/plain"),
    ],
    outputs=[("report", "text/plain")],
)


async def synthesise_findings(action: actionengine.Action):
    topic = await action.get_input("topic").consume()
    brief = await action.get_input("brief").consume()
    report_ids = [
        report_id async for report_id in action.get_input("report_ids")
    ]

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
    response_parts = []
    api_key = await action["api_key"].consume()
    async for response in await get_gemini_client(
        api_key
    ).models.generate_content_stream(
        model="gemini-2.5-flash-preview-05-20",
        contents=prompt,
        config=GenerateContentConfig(
            thinking_config=ThinkingConfig(
                include_thoughts=True,
                thinking_budget=2048,
            ),
            tools=[types.Tool(google_search=GoogleSearch())],
        ),
    ):
        for candidate in response.candidates:
            for part in candidate.content.parts:
                if not part.thought:
                    response_parts.append(part)

    await action["report"].put_and_finalize(
        "".join([part.text for part in response_parts])
    )


SYNTHESISE_FINDINGS_SCHEMA = actionengine.ActionSchema(
    name="synthesise_findings",
    inputs=[
        ("api_key", "text/plain"),
        ("topic", "text/plain"),
        ("brief", "text/plain"),
        ("report_ids", "text/plain"),
    ],
    outputs=[("report", "text/plain")],
)


async def do_deep_research(action: actionengine.Action):
    pass


def make_action_registry():
    registry = actionengine.ActionRegistry()
    registry.register(
        "create_plan",
        CREATE_PLAN_SCHEMA,
        create_plan,
    )
    registry.register(
        "investigate_plan_item",
        INVESTIGATE_PLAN_ITEM_SCHEMA,
        investigate_plan_item,
    )
    registry.register(
        "synthesise_findings",
        SYNTHESISE_FINDINGS_SCHEMA,
        synthesise_findings,
    )
    return registry
