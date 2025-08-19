import asyncio
import json
from contextlib import asynccontextmanager

import actionengine
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

import actions
from actions.http import (
    ActionEngineClient,
    fragment_to_json,
    make_final_node_fragment,
)

description = """
Action Engine HTTP API allows you to interact with Action Engine 
to perform various actions such as text-to-text generation and session 
management.
"""


# Actions need to be registered in an async function, but also before the app
# starts. As there is no explicit "main" function in FastAPI, we use a lifespan
# context manager to handle the initialization of the ActionEngineClient and
# register actions.
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan context manager for the FastAPI application.
    This is used to initialize the ActionEngineClient and register actions.
    """
    try:
        ae = ActionEngineClient.global_instance()
        if ae.get_action_registry() is None:
            ae.set_action_registry(register_actions())
        yield
    finally:
        # Cleanup if necessary
        pass


def get_app_with_handlers() -> FastAPI:
    application = FastAPI(
        title="Action Engine HTTP API",
        description=description,
        lifespan=lifespan,
        root_path="/api",
    )

    application.post(
        "/echo/",
        response_model=str,
        summary="Echo a text string or a list of strings",
        tags=["simple-examples"],
        responses={
            200: {
                "description": "Echoed text",
                "content": {
                    "text/plain": {
                        "example": "Hello, world!",
                    },
                    "application/json": {
                        "example": "Hello, world!",
                    },
                },
            },
            400: {
                "description": "Invalid input",
                "content": {
                    "application/json": {
                        "example": {
                            "detail": "Text must be a string or a list of strings.",
                        },
                    },
                },
            },
        },
    )(actions.echo.http_handler)

    actions.redis.register_http_routes(application)

    return application


def register_actions(
    registry: actionengine.ActionRegistry | None = None,
) -> actionengine.ActionRegistry:
    """
    Create and return an ActionRegistry with the necessary actions registered.
    """
    registry = registry or actionengine.ActionRegistry()
    registry.register("echo", actions.echo.SCHEMA)
    registry.register(
        "generate_content", actions.gemini.GENERATE_CONTENT_SCHEMA
    )
    registry.register(
        "rehydrate_session", actions.gemini.REHYDRATE_SESSION_SCHEMA
    )
    actions.redis.register_actions(registry)
    return registry


def init_app() -> FastAPI:
    """
    Initialize the FastAPI application with the necessary configurations.
    """

    return get_app_with_handlers()


app = init_app()


class SessionInfo(BaseModel):
    session_id: str = Field(
        ...,
        description="The stream ID for the session.",
        examples=["qQB4Y2Rg"],
    )
    next_message_seq: int = Field(
        0,
        description="The sequence number for the next message in the session.",
    )
    next_thought_seq: int = Field(
        0,
        description="The sequence number for the next thought in the session.",
    )


class Message(BaseModel):
    role: str = Field(
        ...,
        description="Role of the message sender, e.g. 'user', "
        "'assistant' or 'system'.",
        examples=["user", "assistant", "system"],
    )
    content: str = Field(
        ...,
        description="Content of the message.",
        examples=[
            "Hello! What can you do?",
            "I can help you with various tasks.",
        ],
    )


class ChatResponse(BaseModel):
    response: str = Field(
        ...,
        description="The generated response from the model.",
        examples=[
            "Hello! I can assist you with a variety of tasks, "
            "including answering questions, providing information, "
            "and more.",
        ],
    )
    thought: str | None = Field(
        None,
        description="The thought process of the model, if available.",
        examples=[
            "The user seems to be interested in what I can do, "
            "so I should provide a brief overview of my capabilities.",
        ],
    )
    session_token: str = Field(
        ...,
        description="The session token for the new position in the session history.",
        examples=["pLA9rUID"],
    )
    gui_url: str | None = Field(
        None,
        description="Optional URL for the GUI to view the session.",
        examples=[
            "https://demos.helena.direct/gemini/?session_token=pLA9rUID&q=ollama",
        ],
    )


class SessionHistoryResponse(BaseModel):
    messages: list[Message] = Field(
        ...,
        description="List of messages in the session history.",
        examples=[
            {
                "role": "user",
                "content": "Hello! What can you do?",
            },
            {
                "role": "assistant",
                "content": "I can assist you with a variety of tasks, "
                "including answering questions, providing information, "
                "and more.",
            },
        ],
    )
    thoughts: list[str] = Field(
        ...,
        description="List of thoughts in the session history.",
        examples=[
            "The user seems to be interested in what I can do, "
            "so I should provide a brief overview of my capabilities.",
        ],
    )


class SendMessageRequest(BaseModel):
    message: str = Field(
        "Hello! What can you do?",
        description="The message to send to the session.",
        examples=[
            "Hello! What can you do?",
            "Can you tell me a joke?",
            "What is the capital of France?",
        ],
    )
    api_key: str | None = Field(
        "ollama",
        description="API key for the model. Use 'ollama' for a local DeepSeek "
        "model, and your Gemini API key for Gemini.",
    )


@app.get(
    "/resolve-token/{session_token}",
    tags=["sessions"],
    response_model=SessionInfo,
    summary="Resolve a session token to get session ID and sequence offsets for raw streams",
)
async def resolve_token(
    session_token: str,
):
    """
    Resolve a session token to get the stream ID and next message/thought
    sequence numbers.

    This information can be used for a more manual handling
    of streams: for example, to read or refer to messages or thoughts as
    separate streams (see `/streams/read`).
    """
    (
        session_id,
        next_message_seq,
        next_thought_seq,
    ) = await actions.gemini.resolve_session_token_to_session_id_and_seqs(
        session_token
    )

    if session_id is None:
        raise HTTPException(
            status_code=404,
            detail="Session token is not found or invalid.",
        )

    return SessionInfo(
        session_id=session_id,
        next_message_seq=next_message_seq,
        next_thought_seq=next_thought_seq,
    )


@app.post(
    "/sessions/",
    tags=["sessions"],
    response_model=ChatResponse,
    summary="Create a new session and send a message to it",
)
async def send_message_to_new_session(
    request: SendMessageRequest,
    stream: bool = False,
):
    """
    Create a new session and send a message to it.
    Your request should contain a new chat message to be sent with the 'user'
    role, and optionally, a Gemini API key.

    Responds with a `ChatResponse` containing the generated response, thought,
    session token, and GUI URL for the new session.
    """
    return await send_message_to_session("new", request, stream=stream)


async def put_node_fragments_in_queue(
    node: actionengine.AsyncNode,
    queue: asyncio.Queue,
    name: str,
):
    seq = 0
    async for data in node:
        fragment = actionengine.NodeFragment(
            id=name,
            chunk=await asyncio.to_thread(actionengine.to_chunk, data),
            seq=seq,
            continued=True,
        )
        await queue.put((name, fragment))
        seq += 1

    # Hacky way to construct a final node fragment
    await queue.put((name, make_final_node_fragment(name, seq)))
    await queue.put(None)


async def make_response_events(
    queue: asyncio.Queue,
):
    """
    Stream node fragments from the queue as a StreamingResponse.
    """

    while True:
        element = await queue.get()
        if element is None:
            break

        name, fragment = element
        # this is just a hack for demo purposes
        include_metadata = fragment.seq == 0 or not fragment.continued
        event = fragment_to_json(fragment, include_metadata=include_metadata)

        yield f"data: {json.dumps(event)}\nevent: session.stream.fragment\n\n"


@app.put(
    "/sessions/{session_token}/",
    tags=["sessions"],
    response_model=ChatResponse,
    summary="Send a message to an existing session",
)
async def send_message_to_session(
    session_token: str,
    request: SendMessageRequest,
    stream: bool = False,
):
    """
    Generate text based on the provided prompt, continuing an existing session
    specified by the session token.

    Returns the response as an event stream
    if `stream` is set to True, otherwise returns a `ChatResponse`.
    """

    ae = ActionEngineClient.global_instance()

    action = ae.make_action("generate_content")
    await action.call()
    await action["prompt"].put_and_finalize(request.message)
    await action["session_token"].put_and_finalize(session_token)
    await action["api_key"].put_and_finalize(request.api_key or "")
    await action["chat_input"].put_and_finalize(request.message)

    if stream:
        queue = asyncio.Queue(maxsize=32)
        print("Starting to stream thoughts and response fragments...")

        stream_thoughts = asyncio.create_task(
            put_node_fragments_in_queue(action["thoughts"], queue, "thought")
        )
        stream_response = asyncio.create_task(
            put_node_fragments_in_queue(action["output"], queue, "response")
        )

        async def make_streaming_response(action: actionengine.Action):
            async for event in make_response_events(queue):
                yield event

            new_session_token = await action["new_session_token"].consume()
            yield f"data: {new_session_token}\nevent: session.session_token\n\n"

            await stream_thoughts
            await stream_response

        return StreamingResponse(
            make_streaming_response(action),
            media_type="text/event-stream",
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
        gui_url=f"https://demos.helena.direct/gemini/"
        f"?session_token={new_session_token}&q={request.api_key or 'ollama'}",
    )


@app.get(
    "/sessions/{session_token}/",
    tags=["sessions"],
    response_model=SessionHistoryResponse,
    summary="Get the latest messages and thoughts from a session up to a given session token",
)
async def get_session_history(session_token: str):
    """
    Get latest session history up to a given session token. The number of
    messages and thoughts returned is now hardcoded.
    """
    ae = ActionEngineClient.global_instance()

    action = ae.make_action("rehydrate_session")
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


@app.get("/sessions/{session_token}/follow", tags=["sessions"])
async def follow_session(
    session_token: str,
    timeout: float = 10.0,
):
    """
    Follow a session to get updates on new messages and thoughts.
    """
    if not session_token:
        raise HTTPException(
            status_code=400,
            detail="Session token is required.",
        )

    if timeout <= 0:
        raise HTTPException(
            status_code=400,
            detail="Timeout must be a non-negative number.",
        )

    if timeout > 300:
        raise HTTPException(
            status_code=400,
            detail="Timeout must not exceed 300 seconds.",
        )

    session_info = await resolve_token(session_token)
    print(f"Following session {session_info.session_id} ")

    registry = ActionEngineClient.global_instance().get_action_registry()
    node_map = actionengine.NodeMap()

    read_messages = registry.make_action("read_store", node_map=node_map).run()
    await read_messages["request"].put_and_finalize(
        actions.redis.ReadStoreRequest(
            key=f"{session_info.session_id}:messages",
            offset=session_info.next_message_seq,
            count=-1,  # Read all messages after the last known message
            timeout=timeout,
        )
    )

    read_thoughts = registry.make_action("read_store", node_map=node_map).run()
    await read_thoughts["request"].put_and_finalize(
        actions.redis.ReadStoreRequest(
            key=f"{session_info.session_id}:thoughts",
            offset=session_info.next_thought_seq,
            count=-1,  # Read all thoughts after the last known thought
            timeout=timeout,
        )
    )

    queue = asyncio.Queue(32)

    async def read_action_output(
        node: actionengine.AsyncNode,
        annotation: str,
        read_timeout: float,
    ):
        try:
            node.set_reader_options(
                timeout=read_timeout,
                ordered=True,
                n_chunks_to_buffer=32,
            )
            async for fragment in node:
                # print(f"Received {annotation} fragment: {fragment}")
                await queue.put((annotation, fragment))
        except asyncio.TimeoutError:
            await queue.put((annotation, None))
        except Exception as exc:
            await queue.put((annotation, exc))
        await queue.put((annotation, None))

    read_messages_task = asyncio.create_task(
        read_action_output(read_messages["response"], "messages", timeout)
    )

    read_thoughts_task = asyncio.create_task(
        read_action_output(read_thoughts["response"], "thoughts", timeout)
    )

    def make_final_event(exc: Exception | None = None):
        token = actions.gemini.make_token_for_session_info(
            session_info.session_id,
            session_info.next_message_seq,
            session_info.next_thought_seq,
        )
        event = {
            "session_token": token,
        }
        if exc is not None:
            event["error"] = str(exc)
        return f"data: {json.dumps(event)}\nevent: session.follow.timeout\n\n"

    async def stream_updates():
        nones_received = 0
        while True:
            try:
                annotation, element = await queue.get()

                if element is None:
                    nones_received += 1
                    if nones_received == 2:
                        # Both queues have been exhausted
                        yield make_final_event()
                        break
                    continue
                if isinstance(element, Exception):
                    # An error occurred while reading from the store
                    # read_messages_task.cancel()
                    # read_thoughts_task.cancel()
                    yield make_final_event(element)
                    break

                if annotation == "messages":
                    element.id = "messages"
                    session_info.next_message_seq = element.seq + 1
                elif annotation == "thoughts":
                    element.id = "thoughts"
                    session_info.next_thought_seq = element.seq + 1

                yield f"data: {json.dumps(fragment_to_json(element))}\n\n"

            except asyncio.TimeoutError:
                yield make_final_event(
                    HTTPException(
                        status_code=408,
                        detail="Timeout while waiting for updates.",
                    )
                )
                break
        await read_messages_task
        await read_thoughts_task

    return StreamingResponse(
        stream_updates(),
        media_type="text/event-stream",
    )
