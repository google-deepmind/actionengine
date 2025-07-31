import asyncio
import dataclasses
import uuid

import evergreen
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

import actions

description = """
Action Engine HTTP API allows you to interact with Action Engine 
to perform various actions such as text-to-text generation and session 
management.
"""


app = FastAPI(
    title="Action Engine HTTP API",
    description=description,
    root_path="/api",
)


@dataclasses.dataclass
class ActionEngineClient:
    stream: evergreen.WireStream
    node_map: evergreen.NodeMap | None = None
    action_registry: evergreen.ActionRegistry | None = None
    session: evergreen.Session | None = None


def make_action_registry() -> evergreen.ActionRegistry:
    """
    Create and return an ActionRegistry with the necessary actions registered.
    """
    registry = evergreen.ActionRegistry()
    registry.register(
        "generate_content", actions.gemini.GENERATE_CONTENT_SCHEMA
    )
    registry.register(
        "rehydrate_session", actions.gemini.REHYDRATE_SESSION_SCHEMA
    )
    return registry


def get_action_engine_client() -> ActionEngineClient:
    if not hasattr(get_action_engine_client, "client"):
        settings = evergreen.get_global_eglt_settings()
        settings.readers_deserialise_automatically = True
        settings.readers_read_in_order = True
        settings.readers_remove_read_chunks = True
        client = ActionEngineClient(
            stream=evergreen.webrtc.make_webrtc_evergreen_stream(
                str(uuid.uuid4()), "demoserver", "demos.helena.direct", 19000
            ),
            node_map=evergreen.NodeMap(),
            action_registry=make_action_registry(),
        )
        client.session = evergreen.Session(
            client.node_map, client.action_registry
        )
        client.session.dispatch_from(client.stream)
        get_action_engine_client.client = client
    return get_action_engine_client.client


def make_action(client: ActionEngineClient, name: str):
    """
    Create an action with the given name using the provided client.
    """
    if not client.action_registry:
        raise ValueError("Action registry is not set in the client.")

    return client.action_registry.make_action(
        name,
        node_map=client.node_map,
        stream=client.stream,
        session=client.session,
    )


class Message(BaseModel):
    role: str = Field(
        ...,
        description="Role of the message sender, e.g. 'user', "
        "'assistant' or 'system'.",
    )
    content: str = Field(
        ...,
        description="Content of the message.",
    )


class ChatResponse(BaseModel):
    response: str = Field(
        ...,
        description="The generated response from the model.",
    )
    thought: str | None = Field(
        None,
        description="The thought process of the model, if available.",
    )
    session_token: str = Field(
        ...,
        description="The session token for the new position in the session history.",
    )
    gui_url: str | None = None


class SessionHistoryResponse(BaseModel):
    messages: list[Message] = Field(
        ...,
        description="List of messages in the session history.",
    )
    thoughts: list[str] = Field(
        ...,
        description="List of thoughts in the session history.",
    )


class SendMessageRequest(BaseModel):
    message: str = Field(
        ...,
        description="The message to send to the session.",
    )
    api_key: str | None = Field(
        None,
        description="API key for the model. Use 'ollama' for a local DeepSeek "
        "model, and your Gemini API key for Gemini.",
    )


@app.post("/sessions/")
async def send_message_to_new_session(
    request: SendMessageRequest,
):
    """
    Create a new session and send a message to it.
    Your request should contain a new chat message to be sent with the 'user'
    role, and optionally, a Gemini API key.

    Responds with a ChatResponse containing the generated response, thought,
    session token, and GUI URL for the new session.
    """
    return await send_message_to_session("new", request)


@app.put(
    "/sessions/{session_token}/",
    response_model=ChatResponse,
)
async def send_message_to_session(
    session_token: str,
    request: SendMessageRequest,
):
    """
    Generate text based on the provided prompt, continuing an existing session
    specified by the session token.
    """
    ae = get_action_engine_client()

    action = make_action(ae, "generate_content")

    await asyncio.gather(
        action.call(),
        action["prompt"].put_and_finalize(
            request.message
        ),  # dummy, not used now
        action["session_token"].put_and_finalize(session_token),
        action["api_key"].put_and_finalize(request.api_key or ""),
        action["chat_input"].put_and_finalize(
            request.message
        ),  # dummy, not used now
    )

    response = ""
    async for text_chunk in action["output"]:
        response += text_chunk

    thought = ""
    async for thought_chunk in action["thoughts"]:
        thought += thought_chunk

    new_session_token = await action["new_session_token"].consume()

    return ChatResponse(
        response=response,
        thought=thought if thought else None,
        session_token=new_session_token if new_session_token else None,
        gui_url=f"https://demos.helena.direct/gemini/?session_token={new_session_token}&q={request.api_key or 'ollama'}",
    )


@app.get("/sessions/{session_token}/")
async def get_session_history(session_token: str):
    """
    Get latest session history up to a given session token.
    """
    ae = get_action_engine_client()

    action = make_action(ae, "rehydrate_session")
    await action.call()
    await action["session_token"].put_and_finalize(session_token)

    messages = []
    idx = 0
    async for message in action["previous_messages"]:
        # Emulate the role based on the index.
        if idx % 2 == 0:
            role = "user"
        else:
            role = "assistant"

        messages.append(Message(role=role, content=message))
        idx += 1

    thoughts = []
    async for thought in action["previous_thoughts"]:
        thoughts.append(thought)

    return SessionHistoryResponse(messages=messages, thoughts=thoughts)
