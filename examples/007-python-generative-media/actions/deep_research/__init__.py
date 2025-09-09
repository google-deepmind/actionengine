import asyncio
import os

import actionengine
from pydantic import BaseModel

from . import create_plan, investigate, synthesise_findings


class DeepResearchAction(BaseModel):
    type: str
    id: str


async def run(action: actionengine.Action):
    try:
        api_key, topic = await asyncio.gather(
            action["api_key"].consume(),
            action["topic"].consume(),
        )
        if api_key == "alpha-demos":
            api_key = os.environ.get("GEMINI_API_KEY")

        print(
            "Got API key and topic, starting deep research workflow", flush=True
        )

        # 1. Create a plan of investigations.
        run_create_plan = action.make_action_in_same_session(
            "create_plan"
        ).run()
        await asyncio.gather(
            run_create_plan["api_key"].put_and_finalize(api_key),
            run_create_plan["topic"].put_and_finalize(topic),
        )
        # Record the action so that it can be inspected later.
        await action["actions"].put(
            DeepResearchAction(type="create_plan", id=run_create_plan.get_id())
        )

        # 2. Collect the plan items and run investigations.
        plan_items = [item async for item in run_create_plan["plan_items"]]
        research_briefs, synthesis_brief = plan_items[:-1], plan_items[-1]

        investigations = []
        for idx, brief in enumerate(research_briefs):
            investigation = action.make_action_in_same_session(
                "investigate"
            ).run()
            await asyncio.gather(
                investigation["api_key"].put_and_finalize(api_key),
                investigation["topic"].put_and_finalize(topic),
                investigation["brief"].put_and_finalize(brief),
            )
            investigations.append(investigation)
            # Record the action for observability / later inspection.
            await action["actions"].put(
                DeepResearchAction(
                    type="investigate", id=investigation.get_id()
                )
            )

        # 3. Synthesize the findings.
        synthesise = action.make_action_in_same_session(
            "synthesise_findings"
        ).run()
        await asyncio.gather(
            synthesise["api_key"].put_and_finalize(api_key),
            synthesise["topic"].put_and_finalize(topic),
            synthesise["brief"].put_and_finalize(synthesis_brief),
        )
        try:
            for investigation in investigations:
                report_id = investigation["report"].get_id()
                await synthesise["report_ids"].put(report_id)
        finally:
            await synthesise["report_ids"].finalize()
        # Record the action for observability / later inspection.
        await action["actions"].put(
            DeepResearchAction(
                type="synthesise_findings", id=synthesise.get_id()
            )
        )

        async for part in synthesise["report"]:
            await action["report"].put(part)

    finally:
        await action["actions"].finalize()
        await action["report"].finalize()


SCHEMA = actionengine.ActionSchema(
    name="deep_research",
    inputs=[
        ("api_key", "text/plain"),
        ("topic", "text/plain"),
    ],
    outputs=[
        ("report", "text/plain"),
        ("actions", DeepResearchAction),
    ],
)


def register_deep_research_actions(
    registry: actionengine.ActionRegistry | None = None,
):
    registry = registry or actionengine.ActionRegistry()
    registry.register(
        "create_plan",
        create_plan.SCHEMA,
        create_plan.run,
    )
    registry.register(
        "investigate",
        investigate.SCHEMA,
        investigate.run,
    )
    registry.register(
        "synthesise_findings",
        synthesise_findings.SCHEMA,
        synthesise_findings.run,
    )
    registry.register(
        "deep_research",
        SCHEMA,
        run,
    )
    return registry
