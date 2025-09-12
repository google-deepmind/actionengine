from fastapi import APIRouter

from ..common import get_global_action_registry
from ..common.discovery import get_action_schemas
from ..common.models import ActionSchema


router = APIRouter(tags=["discovery"])


@router.get("/actions/", response_model=list[ActionSchema])
async def list_actions() -> list[ActionSchema]:
    return get_action_schemas(get_global_action_registry())
