import actionengine

from . import discovery, models, sessions

__all__ = [
    "discovery",
    "models",
    "sessions",
]


_GLOBAL_REGISTRY: actionengine.ActionRegistry | None = None


def get_global_action_registry() -> actionengine.ActionRegistry:
    global _GLOBAL_REGISTRY
    if _GLOBAL_REGISTRY is None:
        _GLOBAL_REGISTRY = actionengine.ActionRegistry()
    return _GLOBAL_REGISTRY


def set_global_action_registry(
    registry: actionengine.ActionRegistry,
) -> None:
    global _GLOBAL_REGISTRY
    _GLOBAL_REGISTRY = registry
