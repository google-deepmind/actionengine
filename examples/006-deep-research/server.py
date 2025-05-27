import asyncio
import os

import evergreen

from deepresearch.serialisation import register_serialisers
from deepresearch.actions import make_action_registry

API_KEY = os.environ.get("GEMINI_API_KEY")


async def main():
    action_registry = make_action_registry()
    node_map = evergreen.NodeMap()

    topic_prompt = input("Enter a topic to perform deep research on: ")

    print("Making a plan.\n")
    action = action_registry.make_action("create_plan", node_map=node_map)

    _ = (
        action.run()
    )  # Keep a reference to the action object to keep the task alive

    await action.get_input("api_key").put_and_finalize(API_KEY)

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
        await action.get_input("api_key").put_and_finalize(API_KEY)
        await action.get_input("topic").put_and_finalize(topic_prompt)
        await action.get_input("brief").put_and_finalize(brief)
        research_actions.append(action)

    print("Requesting final findings:\n")
    print(f"[debug] Synthesis brief: {synthesis_brief}\n")
    synthesis_action = action_registry.make_action(
        "synthesise_findings", node_map=node_map
    )
    _ = synthesis_action.run()
    await synthesis_action.get_input("api_key").put_and_finalize(API_KEY)
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


def test_serialiser(text: str) -> bytes:
    """A simple test serialiser that converts text to bytes."""
    print("py test_serialiser called")
    return text.encode("utf-8")


def test_deserialiser(data: bytes) -> str:
    """A simple test deserialiser that converts bytes to text."""
    print("py test_deserialiser called")
    return data.decode("utf-8")


def sync_main():
    registry = evergreen.data.get_global_serializer_registry()

    registry.register_serializer("text/plain", test_serialiser, str)
    register_serialisers()

    settings = evergreen.get_global_eglt_settings()
    settings.readers_deserialise_automatically = True
    settings.readers_read_in_order = True
    settings.readers_remove_read_chunks = True

    asyncio.run(main())


if __name__ == "__main__":
    sync_main()
