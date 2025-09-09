import asyncio
import os
from typing import cast

import actionengine

from deepresearch.actions import make_action_registry
from deepresearch.actions.deep_research import DeepResearchAction

API_KEY = os.environ.get("GEMINI_API_KEY")


async def print_log_node(node: actionengine.AsyncNode):
    async for item in node:
        print(item, flush=True)


async def observe_nested_action_logs(
    actions_node: actionengine.AsyncNode,
    node_map: actionengine.NodeMap,
):
    async for action in actions_node:
        action = cast(DeepResearchAction, action)
        print(f"Observing logs for action {action.type} ({action.id})")
        asyncio.create_task(print_log_node(node_map[f"{action.id}#user_log"]))


async def main():
    action_registry = make_action_registry()
    node_map = actionengine.NodeMap()

    topic = input("Enter a topic to perform deep research on: ")

    deep_research = action_registry.make_action(
        "deep_research", node_map=node_map
    ).run()
    observe_logs = asyncio.create_task(
        observe_nested_action_logs(deep_research["actions"], node_map)
    )
    await asyncio.gather(
        deep_research["api_key"].put_and_finalize(API_KEY),
        deep_research["topic"].put_and_finalize(topic),
    )

    print(
        "Started deep research action. Observing user logs from nested "
        "actions and waiting for final report.",
        flush=True,
    )

    async for part in deep_research["report"]:
        print(part, end="", flush=True)
    print("\n\nDeep research complete.")

    await observe_logs


def sync_main():
    settings = actionengine.get_global_act_settings()
    settings.readers_deserialise_automatically = True
    settings.readers_read_in_order = True
    settings.readers_remove_read_chunks = True

    asyncio.run(main())


if __name__ == "__main__":
    sync_main()
