import actionengine

from . import create_plan, deep_research, investigate, synthesise_findings


def make_action_registry():
    registry = actionengine.ActionRegistry()
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
        deep_research.SCHEMA,
        deep_research.run,
    )
    return registry
