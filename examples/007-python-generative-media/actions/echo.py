import evergreen


async def run(action: evergreen.Action) -> None:
    response = action.get_output("response")
    try:
        async for message in action.get_input("text"):
            print(message)
            await response.put(message)
    finally:
        await response.finalize()


SCHEMA = evergreen.ActionSchema(
    name="echo",
    inputs=[("text", "text/plain")],
    outputs=[("response", "text/plain")],
)
