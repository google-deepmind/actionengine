import asyncio
import uuid

import actionengine
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import StreamingResponse

from .types import ActionEngineApiKeyHeader
from ..common import get_global_action_registry
from ..common.sessions import PersistentSession, get_session_registry


async def send_outgoing_messages_to_websocket(
    websocket: WebSocket,
    session: PersistentSession,
):
    next_message = None
    try:
        while True:
            next_message = await session.outgoing_messages.get()
            message_bytes = await asyncio.to_thread(next_message.pack_msgpack)
            await websocket.send_bytes(message_bytes)
            next_message = None
    except (WebSocketDisconnect, asyncio.CancelledError):
        # cleanly exit the loop on disconnect or cancellation
        pass
    finally:
        # requeue the message if we are interrupted without sending it
        if next_message is not None:
            await session.outgoing_messages.put(next_message)


async def attach_websocket_to_session(
    websocket: WebSocket,
    session_id: str,
    receive: bool = True,
    ae_api_key: ActionEngineApiKeyHeader = None,
):
    if ae_api_key is not None and ae_api_key.lower() == "wrong-key":
        raise HTTPException(status_code=403, detail="Invalid API key")

    session_registry = get_session_registry()
    try:
        session: PersistentSession = session_registry.get(session_id)
    except KeyError:
        raise HTTPException(status_code=401, detail="Invalid session ID")

    await websocket.accept()

    send_task = None
    if receive:
        send_task = asyncio.create_task(
            send_outgoing_messages_to_websocket(websocket, session)
        )

    try:
        while True:
            message_bytes = await websocket.receive_bytes()
            message = await asyncio.to_thread(
                actionengine.WireMessage.from_msgpack, message_bytes
            )
            await session.incoming_messages.put(message)
    except WebSocketDisconnect:
        print(f"WebSocket disconnected from session `{session_id}`.")
    finally:
        if send_task is not None:
            send_task.cancel()
            await send_task


router = APIRouter(tags=["sessions"])

router.websocket("/{session_id}/")(attach_websocket_to_session)


class CreateSessionResponse(actionengine.pydantic_helpers.BaseModel):
    session_id: str


@router.post("/", response_model=CreateSessionResponse)
async def create_session(
    ae_api_key: ActionEngineApiKeyHeader = None,
) -> CreateSessionResponse:
    if ae_api_key is not None and ae_api_key.lower() == "wrong-key":
        raise HTTPException(status_code=403, detail="Invalid API key")

    session_registry = get_session_registry()
    session_id = str(uuid.uuid4())
    session_registry.get_or_create(session_id, get_global_action_registry())
    return CreateSessionResponse(session_id=session_id)


NON_STREAMING_MAX_LIMIT = 10


@router.get("/{session_id}/")
async def get_wire_messages(
    session_id: str,
    limit: int | None = None,
    stream: bool = False,
    timeout: float = None,
    ae_api_key: ActionEngineApiKeyHeader = None,
):
    print(f"GET /{session_id}/?limit={limit}&stream={stream}&timeout={timeout}")
    if ae_api_key is not None and ae_api_key.lower() == "wrong-key":
        raise HTTPException(status_code=403, detail="Invalid API key")

    session_registry = get_session_registry()
    try:
        session: PersistentSession = session_registry.get(session_id)
    except KeyError:
        raise HTTPException(status_code=401, detail="Invalid session ID")

    limit = limit or NON_STREAMING_MAX_LIMIT
    if limit <= 0:
        raise HTTPException(status_code=400, detail="Limit must be positive")
    if not stream:
        limit = min(limit, NON_STREAMING_MAX_LIMIT)

    timeout = timeout or 300.0
    if timeout <= 0:
        raise HTTPException(status_code=400, detail="Timeout must be positive")
    timeout = min(timeout, 300.0)  # max 5 minutes

    deadline = asyncio.get_event_loop().time() + timeout

    async def message_generator():
        next_message = None
        messages_yielded = 0
        try:
            while messages_yielded < limit or stream:
                time_left = deadline - asyncio.get_event_loop().time()
                if time_left <= 0:
                    break
                try:
                    next_message = await asyncio.wait_for(
                        session.outgoing_messages.get(), timeout=time_left
                    )
                except asyncio.TimeoutError:
                    break
                message_bytes = await asyncio.to_thread(
                    next_message.pack_msgpack
                )
                yield message_bytes
                messages_yielded += 1
        except asyncio.CancelledError:
            # cleanly exit the generator on cancellation
            pass
        finally:
            # requeue the message if we are interrupted without sending it
            if next_message is not None:
                await session.outgoing_messages.put(next_message)

    if stream:

        async def stream_generator():
            async for message in message_generator():
                message_len = len(message)
                int32_message_len = message_len.to_bytes(4, "big")
                chunk = int32_message_len + message
                yield chunk

        return StreamingResponse(
            stream_generator(), media_type="application/octet-stream"
        )

    messages = []
    async for message in message_generator():
        messages.append(message)
    return messages
