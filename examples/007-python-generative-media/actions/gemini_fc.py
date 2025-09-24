import asyncio
import os
import traceback

import ormsgpack

import actionengine
from google import genai
from google.genai import types
from PIL import Image


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

SHIFT_POSITION_SCHEMA = actionengine.ActionSchema(
    name="shift_position",
    inputs=[("request", "application/x-msgpack")],
    outputs=[],
)

TAKE_GL_SCREENSHOT_SCHEMA = actionengine.ActionSchema(
    name="take_screenshot",
    inputs=[],
    outputs=[("screenshot", "image/png")],
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
    registry.register(
        "shift_position",
        SHIFT_POSITION_SCHEMA,
        do_nothing,
    )
    registry.register(
        "take_screenshot",
        TAKE_GL_SCREENSHOT_SCHEMA,
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
                    name="take_screenshot",
                    description="Take a screenshot of the current scene and add it to the conversation. The screenshot will be returned as a subsequent message.",
                    parameters=None,
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
                types.FunctionDeclaration(
                    name="shift_position",
                    description="Shift the entity with i-th entity_id by i-th specified offset.",
                    parameters=genai.types.Schema(
                        type=genai.types.Type.OBJECT,
                        required=["entity_ids", "offsets"],
                        properties={
                            "entity_ids": genai.types.Schema(
                                type=genai.types.Type.ARRAY,
                                items=genai.types.Schema(
                                    type=genai.types.Type.INTEGER,
                                ),
                            ),
                            "offsets": genai.types.Schema(
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
                    "helps users manipulate and inspect 3D scenes. You can use the "
                    "provided functions to get selected entities and set "
                    "their positions, and do other things according to definitions. Be sure to call the functions "
                    "appropriately based on user requests. You can see the scene through screenshots you take. "
                    "If you call take_screenshot, you will get a screenshot in a subsequent message, which you can use to understand the scene. "
                    "Screenshots are for you to see and describe, not for the user to see. "
                    "Only use function calls before you complete the request. If the user asks for something "
                    "subjective, like 'make it look nice', think about what that "
                    "means and do your best to implement it using the available functions. "
                    "Treat object names such as 'blob', 'orb' etc. as a synonym for 'selected entity', and raw numbers as entity IDs. "
                    "Example: 'blob 3' means selected entity with ID 3. 'All blobs' means all selected entities, "
                    "but you must call get_selected_entity_ids to find out which ones they are. "
                    "DO NOT ask for colors. Just come up with ones most suitable. "
                    "You can choose colors and offsets yourself if the user does not specify them exactly. "
                    "Coordinates are aligned with the screen, with +X to the right, +Y up, and +Z towards the viewer, -Z away from the viewer. "
                ),
            ),
        ],
    )


async def execute_prompt(action: actionengine.Action):
    try:
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

            if not turn.candidates:
                done = True
                continue

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
                        with client_action[
                            "response"
                        ].deserialize_automatically(False) as response_node:
                            response_chunk: actionengine.Chunk = (
                                await response_node.consume(timeout=5.0)
                            )
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

                    if "screenshot" in client_action.get_schema().outputs:
                        raw_screenshot: Image.Image = await client_action[
                            "screenshot"
                        ].consume(timeout=5.0)

                        screenshot = Image.new(
                            "RGBA",
                            (raw_screenshot.width, raw_screenshot.height),
                            "WHITE",
                        )

                        screenshot.alpha_composite(raw_screenshot)
                        screenshot = screenshot.convert("RGB")

                        locate = action.make_action_in_same_session(
                            "locate_objects"
                        ).run()
                        await locate["prompt"].put_and_finalize(
                            "amorphous blob"
                        )
                        await locate["image"].put_and_finalize(screenshot)

                        image_bytes = await asyncio.to_thread(
                            actionengine.to_bytes, screenshot, "image/png"
                        )

                        contents.append(
                            types.Content(
                                role="user",
                                parts=[
                                    types.Part.from_bytes(
                                        data=image_bytes,
                                        mime_type="image/png",
                                    )
                                ],
                            )
                        )

            else:
                text = turn.text
                await action["logs"].put(text)
                print(f"- {text}")
                if text.strip().upper().startswith("DONE"):
                    done = True
    except Exception as e:
        traceback.print_exc()
        await action["logs"].put(f"Exception in execute_prompt: {e}")
    else:
        await action["logs"].finalize()


EXECUTE_PROMPT_SCHEMA = actionengine.ActionSchema(
    name="execute_prompt",
    inputs=[("prompt", "text/plain")],
    outputs=[("logs", "text/plain")],
)
