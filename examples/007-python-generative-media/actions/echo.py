import actionengine
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from .http import ActionEngineClient


async def run(action: actionengine.Action) -> None:
    response = action.get_output("response")
    try:
        async for message in action.get_input("text"):
            await response.put(message)
    finally:
        await response.finalize()


SCHEMA = actionengine.ActionSchema(
    name="echo",
    inputs=[("text", "text/plain")],
    outputs=[("response", "text/plain")],
    description='Echoes back the text provided in the input "text" into the output "response", chunk by chunk.',
)


class EchoRequest(BaseModel):
    text: str | list[str] = Field(
        ...,
        description="Text to echo back",
        examples=["Hello, world!", ["Hello", "world!"]],
    )


async def stream_echo_response(action: actionengine.Action):
    async for message in action["response"]:
        yield f"data: {message}\n\n"


async def http_handler(
    request: EchoRequest,
    stream: bool = False,
) -> str | StreamingResponse:
    """
    Echoes back the text provided in the request. If `text` is a list and
    `stream` is True, it streams the response back as a server-sent event
    stream, with a small delay between messages to simulate processing.
    """
    text = request.text
    if isinstance(text, str):
        text = [text]

    ae = ActionEngineClient.global_instance()

    action = ae.make_action("echo")
    await action.call()

    try:
        for message in text:
            await action["text"].put(message)
    finally:
        await action["text"].finalize()

    if stream:
        return StreamingResponse(
            stream_echo_response(action),
            media_type="text/event-stream",
        )

    response = ""
    async for text_chunk in action["response"]:
        response += text_chunk

    return response
