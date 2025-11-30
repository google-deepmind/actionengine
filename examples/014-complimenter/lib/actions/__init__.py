import actionengine

from . import complete_conversation
from . import find_objects_in_image
from . import make_compliment


def make_action_registry():
    registry = actionengine.ActionRegistry()
    complete_conversation.register(registry)
    find_objects_in_image.register(registry)
    make_compliment.register(registry)
    return registry
