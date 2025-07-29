import base64
import os
import traceback

import evergreen
from google import genai
from google.genai import types


def get_gemini_client(api_key: str):
    return genai.client.AsyncClient(
        genai.client.BaseApiClient(
            api_key=api_key,
        )
    )


def get_redis_client():
    if not hasattr(get_redis_client, "_redis_client"):
        get_redis_client._redis_client = evergreen.redis.Redis.connect(
            "localhost"
        )
    return get_redis_client._redis_client


async def rehydrate_session(action: evergreen.Action, put_outputs: bool = True):
    redis_client = get_redis_client()

    session_id = base64.urlsafe_b64encode(os.urandom(6)).decode("utf-8")
    next_message_seq = 0
    next_thought_seq = 0

    session_token = await action["session_token"].consume()
    if not session_token:
        print("No session token provided, nothing to rehydrate.", flush=True)
        return session_id, next_message_seq, next_thought_seq

    try:
        session_info = redis_client.get(session_token)
    except Exception:
        session_info = None
        traceback.print_exc()

    if not session_info:
        print(
            f"Session token {session_token} not found in Redis, "
            f"nothing to rehydrate.",
            flush=True,
        )
        return session_id, next_message_seq, next_thought_seq

    print(
        f"Rehydrating session with token: {session_token}, info: {session_info}",
        flush=True,
    )

    session_info_parts = session_info.split(":")
    session_id = session_info_parts[0]

    next_message_seq = int(session_info_parts[1])
    next_thought_seq = int(session_info_parts[2])

    message_offset = max(0, next_message_seq - 10)
    thought_offset = max(0, next_thought_seq - 10)

    message_store = evergreen.redis.ChunkStore(
        redis_client,
        f"{session_id}:messages",
        -1,
    )
    thought_store = evergreen.redis.ChunkStore(
        redis_client,
        f"{session_id}:thoughts",
        -1,
    )

    if put_outputs:
        try:
            for idx in range(message_offset, next_message_seq):
                await action["previous_messages"].put(message_store.get(idx))

            for idx in range(thought_offset, next_thought_seq):
                await action["previous_thoughts"].put(thought_store.get(idx))
        finally:
            await action["previous_messages"].finalize()
            await action["previous_thoughts"].finalize()

    print(
        f"Rehydrated session {session_id} with {next_message_seq} messages and {next_thought_seq} thoughts.",
        flush=True,
    )

    return session_id, next_message_seq, next_thought_seq


async def run_rehydrate_session(action: evergreen.Action):
    _ = await rehydrate_session(action)


def get_text_chunk(text: str):
    return evergreen.Chunk(
        metadata=evergreen.ChunkMetadata(mimetype="text/plain"),
        data=text.encode("utf-8"),
    )


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
    message_store = evergreen.redis.ChunkStore(
        redis_client,
        f"{session_id}:messages",
        -1,
    )
    thought_store = evergreen.redis.ChunkStore(
        redis_client,
        f"{session_id}:thoughts",
        -1,
    )

    message_store.put(next_message_seq, get_text_chunk(chat_input))
    message_store.put(next_message_seq + 1, get_text_chunk(output))
    next_message_seq += 2

    thought_store.put(next_thought_seq, get_text_chunk(thought))
    next_thought_seq += 1

    session_token = base64.urlsafe_b64encode(os.urandom(6))
    redis_client.set(
        session_token,
        f"{session_id}:{next_message_seq}:{next_thought_seq}",
    )

    return session_token.decode("utf-8")


async def generate_content(action: evergreen.Action):
    print("Running generate_content.", flush=True)

    api_key = await action["api_key"].consume()
    if not api_key:
        await action["output"].put_and_finalize("API key is required.")
        return

    session_id, next_output_seq, next_thought_seq = await rehydrate_session(
        action, put_outputs=False
    )

    if (
        api_key == "i-am-on-the-vip-list"
        or api_key == "i-was-chosen-to-have-access-here"
        or api_key == "alpha-demos"
    ):
        api_key = os.environ.get("GEMINI_API_KEY")

    gemini_client = get_gemini_client(api_key)

    prompt = await action["prompt"].consume()
    chat_input = await action["chat_input"].consume()

    retries_left = 3
    try:
        while retries_left > 0:
            try:
                stream = await gemini_client.models.generate_content_stream(
                    model="gemini-2.5-flash-preview-05-20",
                    contents=prompt,
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
                await action.get_output("new_session_token").put(session_token)
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
    except Exception:
        traceback.print_exc()
    finally:
        await action["output"].finalize()
        await action["thoughts"].finalize()
        await action["new_session_token"].finalize()


REHYDRATE_SESSION_SCHEMA = evergreen.ActionSchema(
    name="rehydrate_session",
    inputs=[
        ("session_token", "text/plain"),
    ],
    outputs=[
        ("previous_messages", "text/plain"),
        ("previous_thoughts", "text/plain"),
    ],
)


GENERATE_CONTENT_SCHEMA = evergreen.ActionSchema(
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
