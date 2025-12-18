import argparse
import asyncio
import os

import actionengine

from lib import actions, models


def setup_action_engine():
    settings = actionengine.get_global_act_settings()
    settings.readers_deserialise_automatically = True
    settings.readers_read_in_order = True
    settings.readers_remove_read_chunks = True


async def sleep_forever():
    while True:
        await asyncio.sleep(1)


async def main(args: argparse.Namespace):
    action_registry = actions.make_action_registry()

    service = actionengine.Service(action_registry)
    server = actionengine.websockets.WebsocketServer(service)

    server.run()
    await models.get_qwen_model_and_processor()
    print("Qwen model and processor loaded.")
    try:
        await sleep_forever()
    except asyncio.CancelledError:
        print("Shutting down Action Engine server.")
        server.cancel()
    finally:
        await asyncio.to_thread(server.join)


def sync_main(args: argparse.Namespace):
    setup_action_engine()
    asyncio.run(main(args), debug=False)


if __name__ == "__main__":
    # logging.basicConfig(level=logging.DEBUG)

    parser = argparse.ArgumentParser(
        description="Run the Action Engine text-to-image server."
    )

    parser.add_argument(
        "--host",
        type=str,
        default="192.168.1.5",
        help="Host address to bind the server to.",
    )
    parser.add_argument(
        "--webrtc-signalling-server",
        type=str,
        default="actionengine.dev",
        help=(
            "WebRTC signalling server address. You may use actionengine.dev "
            "or your own server, but if you use actionengine.dev, please "
            "also set the identity to something unique."
        ),
    )
    parser.add_argument(
        "--webrtc-signalling-port",
        type=int,
        default=19001,
        help="WebRTC signalling server port.",
    )
    parser.add_argument(
        "--webrtc-identity",
        type=str,
        default=os.environ.get("WEBRTC_SIGNALLING_IDENTITY", "complimenter"),
        help="Our ID for the WebRTC signalling server.",
    )

    sync_main(parser.parse_args())
