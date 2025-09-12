import actionengine

from .models import ActionSchema


def get_action_schemas(
    registry: actionengine.ActionRegistry,
) -> list[ActionSchema]:
    schemas = []
    for name in registry.list_registered_actions():
        schemas.append(ActionSchema.copy_from(registry.get_schema(name)))
    return schemas
