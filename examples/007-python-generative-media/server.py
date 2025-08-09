import argparse
import asyncio
import logging
import os

import actionengine

import actions


def make_action_registry():
    registry = actionengine.ActionRegistry()

    registry.register("echo", actions.echo.SCHEMA, actions.echo.run)
    registry.register(
        "rehydrate_session",
        actions.gemini.REHYDRATE_SESSION_SCHEMA,
        actions.gemini.run_rehydrate_session,
    )
    registry.register(
        "generate_content",
        actions.gemini.GENERATE_CONTENT_SCHEMA,
        actions.gemini.generate_content,
    )
    registry.register(
        "text_to_image", actions.text_to_image.SCHEMA, actions.text_to_image.run
    )

    actions.redis.register_actions(registry)

    actionengine.to_bytes(actions.redis.ReadStoreRequest(key=""))
    actionengine.to_bytes(
        actions.redis.WriteRedisStoreRequest(key="", data=b"")
    )

    return registry


def setup_action_engine():
    settings = actionengine.get_global_act_settings()
    settings.readers_deserialise_automatically = True
    settings.readers_read_in_order = True
    settings.readers_remove_read_chunks = True

    # will not be needed later:
    actionengine.to_chunk(
        actions.text_to_image.DiffusionRequest(
            prompt="a hack to get the schema registered for serialization",
        )
    )


async def sleep_forever():
    while True:
        await asyncio.sleep(1)


async def main(args: argparse.Namespace):
    action_registry = make_action_registry()
    service = actionengine.Service(action_registry)
    # server = actionengine.websockets.WebsocketActionEngineServer(service)
    rtc_config = actionengine.webrtc.RtcConfig()
    rtc_config.turn_servers = [
        actionengine.webrtc.TurnServer.from_string(
            "helena:actionengine-webrtc-testing@demos.helena.direct",
        ),
    ]
    server = actionengine.webrtc.WebRtcServer(
        service,
        args.host,
        args.port,
        args.webrtc_signalling_server,
        args.webrtc_signalling_port,
        args.webrtc_identity,
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
        default="0.0.0.0",
        help="Host address to bind the server to.",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=20002,
        help="Port to bind the server to.",
    )
    parser.add_argument(
        "--webrtc-signalling-server",
        type=str,
        default="demos.helena.direct",
        help=(
            "WebRTC signalling server address. You may use demos.helena.direct "
            "or your own server, but if you use demos.helena.direct, please "
            "also set the identity to something unique."
        ),
    )
    parser.add_argument(
        "--webrtc-signalling-port",
        type=int,
        default=19000,
        help="WebRTC signalling server port.",
    )
    parser.add_argument(
        "--webrtc-identity",
        type=str,
        default=os.environ.get("WEBRTC_SIGNALLING_IDENTITY", "demoserver"),
        help="Our ID for the WebRTC signalling server.",
    )
    print(
        "Using WebRTC identity:",
        os.environ.get("WEBRTC_SIGNALLING_IDENTITY", "demoserver"),
    )

    sync_main(parser.parse_args())
