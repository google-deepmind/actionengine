import asyncio
import os
import traceback

import actionengine
from pydantic import BaseModel


class DeepResearchAction(BaseModel):
    type: str
    id: str


async def run(action: actionengine.Action):
    try:
        await action["actions"].put(
            DeepResearchAction(type="deep_research", id=action.get_id())
        )

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
        create_plan = action.make_action_in_same_session("create_plan").run()
        await asyncio.gather(
            create_plan["api_key"].put_and_finalize(api_key),
            create_plan["topic"].put_and_finalize(topic),
        )
        # Record the action so that it can be inspected later.
        await action["actions"].put(
            DeepResearchAction(type="create_plan", id=create_plan.get_id())
        )

        # 2. Collect the plan items and run investigations.
        plan_items = [item async for item in create_plan["plan_items"]]
        research_briefs, synthesis_brief = plan_items[:-1], plan_items[-1]

        # await action["user_log"].put(
        #     f"[deep_research-{action.get_id()}] Scheduling investigations."
        # )

        investigations = []
        for idx, brief in enumerate(research_briefs):
            investigate = action.make_action_in_same_session(
                "investigate"
            ).run()
            investigate.bind_streams_on_outputs_by_default(True)
            await asyncio.gather(
                investigate["api_key"].put_and_finalize(api_key),
                investigate["topic"].put_and_finalize(topic),
                investigate["brief"].put_and_finalize(brief),
            )
            investigations.append(investigate)
            # Record the action for observability / later inspection.
            await action["actions"].put(
                DeepResearchAction(type="investigate", id=investigate.get_id())
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

    except Exception:
        traceback.print_exc()

    finally:
        await action["actions"].finalize()
        await action["report"].finalize()
        # await action["user_log"].put_and_finalize(
        #     f"[deep_research-{action.get_id()}] Deep research complete."
        # )


SCHEMA = actionengine.ActionSchema(
    name="deep_research",
    inputs=[
        ("api_key", "text/plain"),
        ("topic", "text/plain"),
    ],
    outputs=[
        ("report", "text/plain"),
        ("actions", DeepResearchAction),
        ("user_log", "text/plain"),
    ],
)
