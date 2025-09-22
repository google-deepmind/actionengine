import asyncio

import actionengine


async def run_echo(action: actionengine.Action):
    try:
        async for piece in action["text"]:
            await action["response"].put(piece)
    finally:
        await action["response"].finalize()
    raise ValueError("This is a test exception from run_echo")


ECHO_SCHEMA = actionengine.ActionSchema(
    name="echo",
    inputs=[("text", "text/plain")],
    outputs=[("response", "text/plain")],
)


async def main():
    node_map = actionengine.NodeMap()

    echo = actionengine.Action.from_schema(ECHO_SCHEMA, "test-echo")
    echo.bind_handler(run_echo)
    echo.bind_node_map(node_map)
    echo.run()

    for word in ["Hello", "from", "the", "other", "side"]:
        await echo["text"].put(word)
    await echo["text"].finalize()

    try:
        async for part in echo["response"]:
            print(f"Response: {part}")
    except Exception as e:
        print(f"Caught exception from action: {e}")

    # await echo.wait_until_complete()


def sync_main():
    return asyncio.run(main())


if __name__ == "__main__":
    sync_main()
