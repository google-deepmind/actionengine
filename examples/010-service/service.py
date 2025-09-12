from contextlib import asynccontextmanager

from pydantic import BaseModel

import actionengine
from fastapi import FastAPI

import api


async def run_echo(action: actionengine.Action):
    with action["input"].deserialize_automatically(False) as input_node:
        async for chunk in input_node:
            await action["output"].put(chunk)
        await action["output"].finalize()


ECHO_SCHEMA = actionengine.ActionSchema(
    name="echo",
    inputs=[("input", "text/plain")],
    outputs=[("output", "text/plain")],
)


def make_action_registry() -> actionengine.ActionRegistry:
    registry = actionengine.ActionRegistry()
    registry.register("echo", ECHO_SCHEMA, run_echo)
    return registry


@asynccontextmanager
async def lifespan(_: FastAPI):
    try:
        settings = actionengine.get_global_act_settings()
        settings.readers_deserialise_automatically = True
        settings.readers_read_in_order = True
        settings.readers_remove_read_chunks = True

        api.common.set_global_action_registry(make_action_registry())
        api.common.sessions.get_session_registry().get_or_create(
            "test", make_action_registry()
        )
        yield
    finally:
        pass


def _build_fastapi_app() -> FastAPI:
    return api.http.build_fastapi_app(lifespan)


def get_fastapi_app() -> FastAPI:
    if not hasattr(get_fastapi_app, "app"):
        get_fastapi_app.app = _build_fastapi_app()
    return get_fastapi_app.app


app = get_fastapi_app()


class EchoRequest(BaseModel):
    message: str


class EchoResponse(BaseModel):
    message: str


@app.post("/echo", response_model=EchoResponse)
async def echo_endpoint(request: EchoRequest) -> EchoResponse:
    session_registry = api.common.sessions.get_session_registry()
    session = session_registry.get("test")

    action = session.make_action("echo").run()
    for word in request.message.split():
        await action["input"].put(word + " ")
    await action["input"].finalize()

    return EchoResponse(message=request.message)
