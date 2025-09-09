import asyncio
import os

import actionengine

from deepresearch.actions import make_action_registry

API_KEY = os.environ.get("GEMINI_API_KEY")


async def main():
    action_registry = make_action_registry()
    node_map = actionengine.NodeMap()

    topic_prompt = input("Enter a topic to perform deep research on: ")

    print("Making a plan.\n")
    create_plan = action_registry.make_action(
        "create_plan", node_map=node_map
    ).run()
    await asyncio.gather(
        create_plan["api_key"].put_and_finalize(API_KEY),
        create_plan["topic"].put_and_finalize(topic_prompt),
        create_plan["log_prefix"].put_and_finalize("drdebug"),
    )

    plan_items = [item async for item in create_plan["plan_items"]]
    research_briefs, synthesis_brief = plan_items[:-1], plan_items[-1]

    print("Running independent investigations:\n")
    investigations = []
    for idx, brief in enumerate(research_briefs):
        print(f"Investigation {idx + 1}:\n{brief}\n")
        investigation = action_registry.make_action(
            "investigate", node_map=node_map
        ).run()
        await asyncio.gather(
            investigation["api_key"].put_and_finalize(API_KEY),
            investigation["topic"].put_and_finalize(topic_prompt),
            investigation["brief"].put_and_finalize(brief),
        )
        investigations.append(investigation)

    print("Requesting final findings:\n")
    print(f"[debug] Synthesis brief: {synthesis_brief}\n")
    synthesise = action_registry.make_action(
        "synthesise_findings", node_map=node_map
    ).run()
    await asyncio.gather(
        synthesise["api_key"].put_and_finalize(API_KEY),
        synthesise["topic"].put_and_finalize(topic_prompt),
        synthesise["brief"].put_and_finalize(synthesis_brief),
    )
    try:
        for investigation in investigations:
            report_id = investigation["report"].get_id()
            await synthesise["report_ids"].put(report_id)
    finally:
        await synthesise["report_ids"].finalize()

    print("Final report:\n")
    async for part in synthesise["report"]:
        print(part, end="", flush=True)


def sync_main():
    settings = actionengine.get_global_act_settings()
    settings.readers_deserialise_automatically = True
    settings.readers_read_in_order = True
    settings.readers_remove_read_chunks = True

    asyncio.run(main())


if __name__ == "__main__":
    sync_main()
