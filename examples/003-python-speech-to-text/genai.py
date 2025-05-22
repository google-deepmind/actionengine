import asyncio
import os

import evergreen
from evergreen.evergreen import serialisation
from google import genai
from google.genai import types
from google.genai.types import (
    GenerateContentConfig,
    ThinkingConfig,
    GoogleSearch,
)

API_KEY = os.environ.get("GEMINI_API_KEY")

GLOBALLY_DUCT_TAPED_NODE_MAP: evergreen.NodeMap | None = None


def get_gemini_client():
    # if not hasattr(get_gemini_client, "_client"):
    #     get_gemini_client._client = genai.client.AsyncClient(
    #         genai.client.BaseApiClient(
    #             api_key=API_KEY,
    #         )
    #     )
    # return get_gemini_client._client

    return genai.client.AsyncClient(
        genai.client.BaseApiClient(
            api_key=API_KEY,
        )
    )


async def create_plan(action: evergreen.Action):
    topic = await action.get_input("topic").consume()

    response_parts = []

    prompt = (
        f"You are about to do a research on the following topic: {topic}."
        f"You may use tools such as Google "
        "Search if you need. Do not do the research yet. Just think "
        "about your plan and what you will do. Present your "
        "plan clearly step by step, numbering each step, so that they form an "
        "ordered list. Formulate each step as if it was an instruction to "
        "yourself. Do not explain anything yet. Just present your plan. "
        "Start directly with the list, no introduction. The final item of your "
        "plan should start with 'FINALLY: ' and be a detailed instruction "
        "that describes how you will synthesise and present the final result. "
        "Use no more than 5 steps. Make sure that the steps can be "
        "performed independently, because they will be performed by "
        "different agents. "
    )

    async for (
        response
    ) in await get_gemini_client().models.generate_content_stream(
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

    plan_items = action.get_output("plan_items")
    try:
        for line in lines_no_numbers:
            await plan_items.put(line)
            await asyncio.sleep(0.3)  # Simulate some delay
    finally:
        await plan_items.finalize()


CREATE_PLAN_SCHEMA = evergreen.ActionSchema(
    name="create_plan",
    inputs=[("topic", "text/plain")],
    outputs=[("plan_items", "text/plain")],
)


async def investigate_plan_item(action: evergreen.Action):
    topic = await action.get_input("topic").consume()
    brief = await action.get_input("brief").consume()

    print(f"[debug] Investigating: {action.get_input('topic').get_id()}")

    prompt = (
        f'As part of a bigger project that researches "{topic}", you are '
        f"given the following brief: {brief}. Please investigate "
        "online sources, making sure to use Google Search, and making necessary "
        "citations. Present your findings in a clear and concise manner, "
        "using bullet points. Make sure to include the sources you used. In the "
        "beginning of your report, concisely mention the brief you were given. "
    )

    response_parts = []
    async for (
        response
    ) in await get_gemini_client().models.generate_content_stream(
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


INVESTIGATE_PLAN_ITEM_SCHEMA = evergreen.ActionSchema(
    name="investigate_plan_item",
    inputs=[
        ("topic", "text/plain"),
        ("brief", "text/plain"),
    ],
    outputs=[("report", "text/plain")],
)


async def synthesise_findings(action: evergreen.Action):
    topic = await action.get_input("topic").consume()
    brief = await action.get_input("brief").consume()
    report_ids = [
        report_id async for report_id in action.get_input("report_ids")
    ]

    reports: list[str] = await asyncio.gather(
        *(
            GLOBALLY_DUCT_TAPED_NODE_MAP.get(report_id).consume()
            for report_id in report_ids
        )
    )
    print("All reports have been received.")

    prompt = (
        f"You are about to synthesise the findings of a research on the "
        f"following topic: {topic}. You have the following {len(reports)} "
        f"reports: {'\n\n'.join(reports)}\n\n NOW, {brief}"
    )
    response_parts = []
    async for (
        response
    ) in await get_gemini_client().models.generate_content_stream(
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


SYNTHESISE_FINDINGS_SCHEMA = evergreen.ActionSchema(
    name="synthesise_findings",
    inputs=[
        ("topic", "text/plain"),
        ("brief", "text/plain"),
        ("report_ids", "text/plain"),
    ],
    outputs=[("report", "text/plain")],
)


def str_to_bytes(text: str) -> bytes:
    return text.encode("utf-8")


def bytes_to_str(data: bytes) -> str:
    return data.decode("utf-8")


def register_serialisers(
    registry: serialisation.SerialiserRegistry | None = None,
):
    registry = registry or serialisation.get_global_serialiser_registry()

    registry.register_serialiser(str_to_bytes, "text/plain", str)
    registry.register_deserialiser(bytes_to_str, "text/plain", str)


def make_action_registry():
    registry = evergreen.ActionRegistry()
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


async def main():
    global GLOBALLY_DUCT_TAPED_NODE_MAP

    action_registry = make_action_registry()
    GLOBALLY_DUCT_TAPED_NODE_MAP = evergreen.NodeMap()
    node_map = GLOBALLY_DUCT_TAPED_NODE_MAP

    topic_prompt = input("Enter a topic to perform deep research on: ")

    print("Making a plan.\n")
    action = action_registry.make_action("create_plan", node_map=node_map)

    _ = (
        action.run()
    )  # Keep a reference to the action object to keep the task alive

    topic = action.get_input("topic")
    await topic.put_and_finalize(topic_prompt)

    plan_items = [item async for item in action.get_output("plan_items")]
    research_briefs, synthesis_brief = plan_items[:-1], plan_items[-1]

    print("Running independent investigations:\n")
    research_actions = []
    for idx, brief in enumerate(research_briefs):
        print(f"Investigation {idx + 1}:\n{brief}\n")
        action = action_registry.make_action(
            "investigate_plan_item", node_map=node_map
        )
        _ = action.run()
        await action.get_input("topic").put_and_finalize(topic_prompt)
        await action.get_input("brief").put_and_finalize(brief)
        research_actions.append(action)

    print("Requesting final findings:\n")
    synthesis_action = action_registry.make_action(
        "synthesise_findings", node_map=node_map
    )
    _ = synthesis_action.run()
    await synthesis_action.get_input("topic").put_and_finalize(topic_prompt)
    await synthesis_action.get_input("brief").put_and_finalize(synthesis_brief)
    try:
        for action in research_actions:
            report_id = action.get_output("report").get_id()
            await synthesis_action.get_input("report_ids").put(report_id)
    finally:
        await synthesis_action.get_input("report_ids").finalize()

    final_report = await synthesis_action.get_output("report").consume()
    print(f"Final report:\n\n{final_report}")


def sync_main():
    register_serialisers()

    settings = evergreen.get_global_eglt_settings()
    settings.readers_deserialise_automatically = True
    settings.readers_read_in_order = True
    settings.readers_remove_read_chunks = True

    asyncio.run(main())


if __name__ == "__main__":
    sync_main()
