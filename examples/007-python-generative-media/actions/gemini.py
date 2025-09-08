import asyncio
import base64
import os
import random
import traceback

import actionengine
from google import genai
from google.genai import types
from ollama import chat, Options


def get_gemini_client(api_key: str):
    return genai.client.AsyncClient(
        genai.client.BaseApiClient(
            api_key=api_key,
        )
    )


def get_redis_client():
    if not hasattr(get_redis_client, "_redis_client"):
        get_redis_client._redis_client = actionengine.redis.Redis.connect(
            "localhost"
        )
    return get_redis_client._redis_client


async def resolve_session_token_to_session_id_and_seqs(
    token: str = "",
) -> tuple[str | None, int, int]:
    redis_client = get_redis_client()

    session_id = base64.urlsafe_b64encode(os.urandom(6)).decode("utf-8")
    next_message_seq = 0
    next_thought_seq = 0

    if not token:
        print(
            "No session token provided, nothing to resolve. "
            "Generating a new session ID.",
            flush=True,
        )
        return session_id, next_message_seq, next_thought_seq

    try:
        session_info = await asyncio.to_thread(redis_client.get, token)
    except Exception:
        session_info = None
        traceback.print_exc()

    if not session_info:
        print(f"Session token {token} not found in Redis.", flush=True)
        return None, next_message_seq, next_thought_seq

    print(
        f"Resolved session with token: {token}, info: {session_info}",
        flush=True,
    )

    session_info_parts = session_info.split(":")
    session_id = session_info_parts[0]

    next_message_seq = int(session_info_parts[1])
    next_thought_seq = int(session_info_parts[2])

    return session_id, next_message_seq, next_thought_seq


async def run_rehydrate_session(action: actionengine.Action):
    session_token = await action["session_token"].consume()
    session_id, next_message_seq, next_thought_seq = (
        await resolve_session_token_to_session_id_and_seqs(session_token)
    )
    if session_id is None:
        session_id = base64.urlsafe_b64encode(os.urandom(6)).decode("utf-8")

    redis_client = get_redis_client()

    # message offset is 2x the thought offset, because there is a user input
    # and a model output for each turn, but only one thought message per turn.
    message_offset = max(0, next_message_seq - 20)
    thought_offset = max(0, next_thought_seq - 10)

    message_store = actionengine.redis.ChunkStore(
        redis_client,
        f"{session_id}:messages",
        -1,  # no TTL
    )
    thought_store = actionengine.redis.ChunkStore(
        redis_client,
        f"{session_id}:thoughts",
        -1,  # no TTL
    )

    try:
        for idx in range(message_offset, next_message_seq):
            await action["previous_messages"].put(message_store.get(idx))

        for idx in range(thought_offset, next_thought_seq):
            await action["previous_thoughts"].put(thought_store.get(idx))
    finally:
        await action["previous_messages"].finalize()
        await action["previous_thoughts"].finalize()

    print(
        f"Rehydrated session {session_id} with {next_message_seq} messages "
        f"and {next_thought_seq} thoughts.",
        flush=True,
    )


def get_text_chunk(text: str):
    return actionengine.Chunk(
        metadata=actionengine.ChunkMetadata(mimetype="text/plain"),
        data=text.encode("utf-8"),
    )


def make_token_for_session_info(
    session_id: str,
    next_message_seq: int,
    next_thought_seq: int,
) -> str:
    redis_client = get_redis_client()
    session_token = base64.urlsafe_b64encode(os.urandom(6)).decode("utf-8")
    redis_client.set(
        session_token,
        f"{session_id}:{next_message_seq}:{next_thought_seq}",
    )
    return session_token


async def save_message_turn(
    session_id: str,
    chat_input: str,
    output: str,
    thought: str,
    next_message_seq: int = 0,
    next_thought_seq: int = 0,
):
    print(
        f"Saving message turn for session {session_id}, "
        f"next_message_seq: {next_message_seq}, next_thought_seq: {next_thought_seq}",
        flush=True,
    )
    redis_client = get_redis_client()
    message_store = actionengine.redis.ChunkStore(
        redis_client,
        f"{session_id}:messages",
        -1,  # no TTL
    )

    await asyncio.to_thread(
        message_store.put, next_message_seq, get_text_chunk(chat_input)
    )
    await asyncio.to_thread(
        message_store.put, next_message_seq + 1, get_text_chunk(output)
    )
    next_message_seq += 2

    if thought:
        thought_store = actionengine.redis.ChunkStore(
            redis_client,
            f"{session_id}:thoughts",
            -1,  # no TTL
        )
        await asyncio.to_thread(
            thought_store.put, next_thought_seq, get_text_chunk(thought)
        )
        next_thought_seq += 1

    session_token = await asyncio.to_thread(
        make_token_for_session_info,
        session_id,
        next_message_seq,
        next_thought_seq,
    )

    return session_token


async def generate_content_gemini(
    action: actionengine.Action,
    api_key: str = "",
):
    session_token = await action["session_token"].consume()
    session_id, next_output_seq, next_thought_seq = (
        await resolve_session_token_to_session_id_and_seqs(session_token)
    )
    if session_id is None:
        session_id = base64.urlsafe_b64encode(os.urandom(6)).decode("utf-8")

    rehydrate_action = (
        action.get_registry()
        .make_action(
            "rehydrate_session",
            node_map=action.get_node_map(),
            stream=None,  # No stream needed for this action
        )
        .run()
    )
    await rehydrate_action["session_token"].put_and_finalize(session_token)

    contents = []
    message_idx = 0
    async for message in rehydrate_action["previous_messages"]:
        contents.append(
            types.Content(
                parts=[types.Part(text=message)],
                role="user" if len(contents) % 2 == 0 else "model",
            )
        )
        message_idx += 1

    gemini_client = get_gemini_client(api_key)

    chat_input = await action["chat_input"].consume()

    contents.append(
        types.Content(parts=[types.Part(text=chat_input)], role="user")
    )

    try:
        retries_left = 3
        while retries_left > 0:
            try:
                stream = await gemini_client.models.generate_content_stream(
                    model="gemini-2.5-flash-preview-05-20",
                    contents=contents,
                    config=types.GenerateContentConfig(
                        thinking_config=types.ThinkingConfig(
                            include_thoughts=True,
                            thinking_budget=-1,
                        ),
                        tools=[types.Tool(google_search=types.GoogleSearch())],
                    ),
                )

                output = ""
                thought = ""

                async for chunk in stream:
                    for candidate in chunk.candidates:
                        if not candidate.content:
                            continue
                        if not candidate.content.parts:
                            continue

                        for part in candidate.content.parts:
                            if not part.thought:
                                await action["output"].put(part.text)
                                output += part.text
                            else:
                                await action["thoughts"].put(part.text)
                                thought += part.text

                session_token = await save_message_turn(
                    session_id,
                    chat_input,
                    output,
                    thought,
                    next_output_seq,
                    next_thought_seq,
                )
                await action["new_session_token"].put_and_finalize(
                    session_token
                )
                break
            except Exception:
                retries_left -= 1
                await action["output"].put(
                    f"Retrying due to an internal error... {retries_left} retries left."
                )
                if retries_left == 0:
                    await action["output"].put(
                        "Failed to connect to Gemini API."
                    )
                    traceback.print_exc()
                    return
    finally:
        await action["output"].finalize()
        await action["thoughts"].finalize()


async def generate_content_ollama(action: actionengine.Action):
    session_token = await action["session_token"].consume()
    session_id, next_output_seq, next_thought_seq = (
        await resolve_session_token_to_session_id_and_seqs(session_token)
    )
    if session_id is None:
        session_id = base64.urlsafe_b64encode(os.urandom(6)).decode("utf-8")

    rehydrate_action = (
        action.get_registry()
        .make_action(
            "rehydrate_session",
            node_map=action.get_node_map(),
            stream=None,  # No stream needed for this action
        )
        .run()
    )
    await rehydrate_action["session_token"].put_and_finalize(session_token)

    messages = [
        {
            "role": "system",
            "content": ""
            "You are a helpful assistant called DeepSeek. "
            "You are running on Helena's local server using Ollama. ",
        },
    ]
    message_idx = 0
    async for message in rehydrate_action["previous_messages"]:
        if message_idx % 2 == 0:
            messages.append({"role": "user", "content": message})
        else:
            messages.append({"role": "assistant", "content": message})
        message_idx += 1

    thoughts = []
    async for thought in rehydrate_action["previous_thoughts"]:
        thoughts.append(thought)

    chat_input = await action["chat_input"].consume()
    messages.append({"role": "user", "content": chat_input})

    try:
        retries_left = 3
        while retries_left > 0:
            try:
                stream = chat(
                    model="deepseek-r1:8b",
                    messages=messages,
                    stream=True,
                    think=True,
                    options=Options(seed=random.randint(0, 2**31 - 1)),
                )

                output = ""
                thought = ""

                for chunk in stream:
                    if "content" in chunk["message"]:
                        await action["output"].put(chunk["message"]["content"])
                        output += chunk["message"]["content"]

                    if "thinking" in chunk["message"]:
                        await action["thoughts"].put(
                            chunk["message"]["thinking"]
                        )
                        thought += chunk["message"]["thinking"]

                session_token = await save_message_turn(
                    session_id,
                    chat_input,
                    output,
                    thought,
                    next_output_seq,
                    next_thought_seq,
                )
                await action["new_session_token"].put_and_finalize(
                    session_token
                )
                break
            except Exception:
                retries_left -= 1
                await action["output"].put(
                    f"Retrying due to an internal error... {retries_left} retries left."
                )
                if retries_left == 0:
                    await action["output"].put(
                        "Failed to connect to Gemini API."
                    )
                    traceback.print_exc()
                    return
    finally:
        await action["output"].finalize()
        await action["thoughts"].finalize()


async def generate_content(action: actionengine.Action):
    print("Running generate_content.", flush=True)

    api_key = await action["api_key"].consume(timeout=3.0)
    if not api_key:
        await action["output"].put_and_finalize("API key is required.")
        return

    if (
        api_key == "i-am-on-the-vip-list"
        or api_key == "i-was-chosen-to-have-access-here"
        or api_key == "alpha-demos"
    ):
        api_key = os.environ.get("GEMINI_API_KEY")

    if api_key not in ("", "ollama"):
        await generate_content_gemini(action, api_key)
    else:
        await generate_content_ollama(action)


REHYDRATE_SESSION_SCHEMA = actionengine.ActionSchema(
    name="rehydrate_session",
    inputs=[
        ("session_token", "text/plain"),
    ],
    outputs=[
        ("previous_messages", "text/plain"),
        ("previous_thoughts", "text/plain"),
    ],
)


GENERATE_CONTENT_SCHEMA = actionengine.ActionSchema(
    name="generate_content",
    inputs=[
        ("api_key", "text/plain"),
        ("prompt", "text/plain"),
        ("chat_input", "text/plain"),
        ("session_token", "text/plain"),
    ],
    outputs=[
        ("output", "text/plain"),
        ("thoughts", "text/plain"),
        ("new_session_token", "text/plain"),
    ],
)
