import asyncio
import os

import ormsgpack

import actionengine
from google import genai
from google.genai import types


async def do_nothing():
    pass


GET_SELECTED_ENTITY_IDS_SCHEMA = actionengine.ActionSchema(
    name="get_selected_entity_ids",
    inputs=[],
    outputs=[("response", "application/x-msgpack")],
)

SET_COLOR_SCHEMA = actionengine.ActionSchema(
    name="set_color",
    inputs=[("request", "application/x-msgpack")],
    outputs=[],
)

SET_POSITION_SCHEMA = actionengine.ActionSchema(
    name="set_position",
    inputs=[("request", "application/x-msgpack")],
    outputs=[],
)


def make_client_action_registry() -> actionengine.ActionRegistry:
    registry = actionengine.ActionRegistry()
    registry.register(
        "get_selected_entity_ids",
        GET_SELECTED_ENTITY_IDS_SCHEMA,
        do_nothing,
    )
    registry.register(
        "set_color",
        SET_COLOR_SCHEMA,
        do_nothing,
    )
    registry.register(
        "set_position",
        SET_POSITION_SCHEMA,
        do_nothing,
    )
    return registry


def setup_action_engine():
    settings = actionengine.get_global_act_settings()
    settings.readers_remove_read_chunks = False
    settings.readers_deserialise_automatically = False


def get_client():
    if not hasattr(get_client, "client"):
        get_client.client = genai.Client(
            api_key=os.environ.get("GEMINI_API_KEY"),
        )
    return get_client.client


def get_tools() -> list[types.Tool]:
    return [
        types.Tool(
            function_declarations=[
                types.FunctionDeclaration(
                    name="get_selected_entity_ids",
                    description="Retrieve the list of entity IDs of entities currently selected in the UI. Returns a list of integers.",
                ),
                types.FunctionDeclaration(
                    name="set_color",
                    description="Set the color of entity with i-th entity_id to i-th target color. The color is a hexadecimal string, e.g. #rrggbb.",
                    parameters=genai.types.Schema(
                        type=genai.types.Type.OBJECT,
                        required=["entity_ids", "colors"],
                        properties={
                            "entity_ids": genai.types.Schema(
                                type=genai.types.Type.ARRAY,
                                items=genai.types.Schema(
                                    type=genai.types.Type.INTEGER,
                                ),
                            ),
                            "colors": genai.types.Schema(
                                type=genai.types.Type.ARRAY,
                                items=genai.types.Schema(
                                    type=genai.types.Type.STRING,
                                ),
                            ),
                        },
                    ),
                ),
                types.FunctionDeclaration(
                    name="set_position",
                    description="Move the entity with i-th entity_id to i-th specified position.",
                    parameters=genai.types.Schema(
                        type=genai.types.Type.OBJECT,
                        required=["entity_ids", "positions"],
                        properties={
                            "entity_ids": genai.types.Schema(
                                type=genai.types.Type.ARRAY,
                                items=genai.types.Schema(
                                    type=genai.types.Type.INTEGER,
                                ),
                            ),
                            "positions": genai.types.Schema(
                                type=genai.types.Type.ARRAY,
                                items=genai.types.Schema(
                                    type=genai.types.Type.ARRAY,
                                    items=genai.types.Schema(
                                        type=genai.types.Type.NUMBER,
                                    ),
                                ),
                            ),
                        },
                    ),
                ),
            ]
        )
    ]


def get_config(tools: list[types.Tool]) -> types.GenerateContentConfig:
    return types.GenerateContentConfig(
        thinking_config=types.ThinkingConfig(
            thinking_budget=1024,
        ),
        tools=tools,
        system_instruction=[
            types.Part.from_text(
                text=(
                    "You are an assistant that "
                    "helps users manipulate 3D scenes. You can use the "
                    "provided functions to get selected entities and set "
                    "their positions. Be sure to call the functions "
                    "appropriately based on user requests. Do not respond with messages, only use function calls. "
                    "Only use function calls before you complete the request. Once you "
                    "have completed the user's request and your function calls "
                    "have returned successfully, respond with a message "
                    "DONE, just the word DONE. If the user asks for something "
                    "subjective, like 'make it look nice', think about what that "
                    "means and do your best to implement it using the available functions."
                ),
            ),
        ],
    )


async def execute_prompt(action: actionengine.Action):
    client = get_client()

    with action["prompt"].deserialize_automatically() as prompt_node:
        prompt = await prompt_node.consume(timeout=5.0)

    model = "gemini-2.5-flash-lite"
    contents = [
        types.Content(
            role="user",
            parts=[types.Part.from_text(text=prompt)],
        )
    ]

    client_action_registry = make_client_action_registry()

    done = False
    while not done:
        turn = client.models.generate_content(
            model=model, contents=contents, config=get_config(get_tools())
        )

        content = turn.candidates[0].content
        contents.append(content)

        if turn.function_calls is not None:
            for fc in turn.function_calls:
                if not client_action_registry.is_registered(fc.name):
                    print(f"- Function {fc.name} not registered, skipping")
                    continue

                print(f"- Function call: {fc.name} with args {fc.args}")
                client_action = client_action_registry.make_action(
                    fc.name,
                    node_map=action.get_node_map(),
                    stream=action.get_stream(),
                    session=action.get_session(),
                )
                await client_action.call()
                if fc.args:
                    request_data = await asyncio.to_thread(
                        ormsgpack.packb, fc.args
                    )
                    await client_action["request"].put_and_finalize(
                        actionengine.Chunk(
                            data=request_data,
                            metadata=actionengine.ChunkMetadata(
                                mimetype="application/x-msgpack"
                            ),
                        )
                    )
                # this is a hack until javascript client supports proper statuses:
                await asyncio.sleep(0.15)

                response = None
                if "response" in client_action.get_schema().outputs:
                    response_chunk: actionengine.Chunk = await client_action[
                        "response"
                    ].consume(timeout=5.0)
                    response = await asyncio.to_thread(
                        ormsgpack.unpackb, response_chunk.data
                    )

                contents.append(
                    types.Content(
                        role="user",
                        parts=[
                            types.Part.from_function_response(
                                name=fc.name,
                                response={"result": response},
                            )
                        ],
                    )
                )
                print(f"- Function {fc.name} returned {response}")

        else:
            text = turn.text
            print(f"- {text}")
            if text.strip().upper().startswith("DONE"):
                done = True


async def sleep_forever():
    while True:
        await asyncio.sleep(1)


async def main():
    action_registry = actionengine.ActionRegistry()
    action_registry.register(
        "execute_prompt",
        actionengine.ActionSchema(
            name="execute_prompt",
            inputs=[("prompt", "text/plain")],
            outputs=[],
        ),
        execute_prompt,
    )

    service = actionengine.Service(action_registry)
    # server = actionengine.websockets.WebsocketActionEngineServer(service)
    rtc_config = actionengine.webrtc.RtcConfig()
    rtc_config.turn_servers = [
        actionengine.webrtc.TurnServer.from_string(
            "helena:actionengine-webrtc-testing@actionengine.dev",
        ),
    ]
    server = actionengine.webrtc.WebRtcServer(
        service,
        "0.0.0.0",
        20002,
        "actionengine.dev",
        19000,
        "bidi-demo",
        rtc_config,
    )

    server.run()
    try:
        await sleep_forever()
    except asyncio.CancelledError:
        print("Shutting down Action Engine server.")
        server.cancel()
    finally:
        await asyncio.to_thread(server.join)


def sync_main():
    setup_action_engine()
    return asyncio.run(main())


if __name__ == "__main__":
    sync_main()
