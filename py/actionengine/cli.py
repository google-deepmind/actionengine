import argparse
import asyncio
import importlib
import logging
import os
import sys

import actionengine
import requests


API_ROOT = "https://actionengine.dev/api"

logger = logging.getLogger(__name__)


def setup_action_engine():
    settings = actionengine.get_global_act_settings()
    settings.readers_deserialise_automatically = True
    settings.readers_read_in_order = True
    settings.readers_remove_read_chunks = True


async def


async def sleep_forever():
    while True:
        await asyncio.sleep(1)


async def serve(
    registry: actionengine.ActionRegistry,
    host: str,
    api_key: str,
    timed_token: str,
):
    service = actionengine.Service(registry)
    rtc_config = actionengine.webrtc.RtcConfig()
    rtc_config.turn_servers = [
        actionengine.webrtc.TurnServer.from_string(
            "helena:actionengine-webrtc-testing@actionengine.dev",
        ),
    ]
    server = actionengine.webrtc.WebRtcServer.create(
        service,
        "0.0.0.0",
        20002,
        f"{host}",
        f"wss://actionengine.dev:19001",
        rtc_config,
    )

    server.set_signalling_header("X-API-Key", api_key)
    server.set_signalling_header("X-Timed-Peer-Token", timed_token)

    server.run()
    logger.info("Action Engine WebRTC server is running.")
    logger.info(f"Host name: {host}")
    logger.info(
        f"You can now verify the host is online by checking out its exposed actions: "
    )
    logger.info(f"curl -X 'GET' https://actions.live/{host}")
    logger.info("Alternatively, just visit the URL in your browser.")
    logger.info("Waiting for connections from now on.")

    try:
        await sleep_forever()
    except asyncio.CancelledError:
        logger.info("Shutting down Action Engine server.")
        server.cancel()
    finally:
        await asyncio.to_thread(server.join)


async def serve_command(args: argparse.Namespace):
    registry_path = args.registry

    # Dynamically import the action registry
    module_path, symbol_name = registry_path.rsplit(":", 1)
    try:
        # Ensure current working directory is on sys.path so dotted modules
        # can be resolved relative to where the user runs the command.
        cwd = os.getcwd()
        if cwd not in sys.path:
            sys.path.insert(0, cwd)

        module = importlib.import_module(module_path)
    except ImportError as e:
        # As a fallback, allow specifying the module as a file path relative to CWD,
        # e.g. "path/to/module.py:registry" or "path/to/package:registry".
        try:
            from importlib import util as importlib_util
            import pathlib

            candidate = pathlib.Path(module_path)
            if not candidate.is_absolute():
                candidate = pathlib.Path(cwd) / candidate

            if candidate.is_dir():
                # Try to load a package (with __init__.py if present)
                init_py = candidate / "__init__.py"
                location = init_py if init_py.exists() else candidate
            else:
                location = candidate

            if not location.exists():
                raise

            spec = importlib_util.spec_from_file_location(
                f"user_registry_module_{abs(hash(str(location)))%10_000}",
                str(location),
            )
            if spec is None or spec.loader is None:
                raise
            module = importlib_util.module_from_spec(spec)
            spec.loader.exec_module(module)  # type: ignore[attr-defined]
        except Exception:
            # Re-raise the original error with clearer context.
            raise ImportError(
                f"Could not import module '{module_path}' relative to CWD '{cwd}': {e}"
            ) from e
    action_registry = getattr(module, symbol_name)

    if not isinstance(action_registry, actionengine.ActionRegistry):
        raise ValueError(
            f"The specified registry '{registry_path}' is not an "
            "instance of ActionRegistry."
        )

    api_key_info_response: requests.Response = await asyncio.to_thread(
        requests.get,
        f"{API_ROOT}/auth/api-keys/get-info/{args.api_key}",
        headers={"X-API-Key": args.api_key},
    )
    api_key_info_response.raise_for_status()
    api_key_info_data = await asyncio.to_thread(api_key_info_response.json)
    owner_username = api_key_info_data.get("owner_username")
    if not owner_username:
        raise ValueError("API key is invalid or has no associated user.")
    logger.info(
        f"API key is valid for user: {api_key_info_data.get('owner_username')}"
    )

    timed_token = None
    full_host_name = f"@{owner_username}:{args.host}"

    # First, try to get an active timed token from the API
    response: requests.Response = await asyncio.to_thread(
        requests.get,
        f"{API_ROOT}/hosts/{full_host_name}/get-active-timed-tokens",
        headers={"X-API-Key": args.api_key},
    )
    if response.ok:
        response_data = await asyncio.to_thread(response.json)
        if not response_data:
            raise ValueError("No active timed tokens found.")
        timed_token = response_data[0].get("token")
        if not timed_token:
            raise ValueError("Timed token not found in response.")

    if timed_token:
        logger.info(f"Retrieved a timed token for host. Starting the server.")
        return await serve(
            action_registry,
            full_host_name,
            args.api_key,
            timed_token,
        )

    logger.info(
        "Could not retrieve timed token from API, assuming host "
        "does not exist. Will try to create it."
    )
    create_host_response: requests.Response = await asyncio.to_thread(
        requests.post,
        f"{API_ROOT}/hosts/",
        headers={"X-API-Key": args.api_key},
        json={
            "name": args.host,
            "display_name": args.display_name or f"Host for {args.host}",
            "scoped": True,
            "description": args.description or "",
        },
    )
    if not create_host_response.ok:
        raise ValueError(f"Failed to create host: {create_host_response.text}")
    create_host_data = await asyncio.to_thread(create_host_response.json)
    logger.info(f"Host created: {create_host_data}")

    create_timed_token_response: requests.Response = await asyncio.to_thread(
        requests.post,
        f"{API_ROOT}/hosts/{full_host_name}/create-timed-token",
        headers={"X-API-Key": args.api_key},
        params={"ttl": 3600 * 24 * 7},  # 7 days
    )
    create_timed_token_response.raise_for_status()

    create_timed_token_data = await asyncio.to_thread(
        create_timed_token_response.json
    )
    timed_token = create_timed_token_data.get("token")
    if not timed_token:
        raise ValueError("Timed token not found in response.")
    logger.info(f"Timed token created: {timed_token}")

    return await serve(
        action_registry,
        full_host_name,
        args.api_key,
        timed_token,
    )


async def main(args: argparse.Namespace):
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)-8s %(message)s")
    )
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    setup_action_engine()

    if args.command == "serve":
        return await serve_command(args)

    raise ValueError(f"Unknown command: {args.command}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Health check for Action Engine host."
    )
    parser.add_argument(
        "command",
        help="The command to execute.",
        choices=["serve"],
    )
    parser.add_argument(
        "--api-key",
        type=str,
        help="API key for authenticating with the Action Engine API.",
    )
    parser.add_argument(
        "--host",
        type=str,
        help="The name of the scoped Action Engine host.",
    )
    parser.add_argument(
        "--display-name",
        type=str,
        required=False,
        help="Display name for the Action Engine host.",
    )
    parser.add_argument(
        "--description",
        type=str,
        required=False,
        help="Description for the Action Engine host.",
    )
    parser.add_argument(
        "--registry",
        type=str,
        help="The action registry to host. This should be an importable "
        "path of a symbol in a Python module.",
    )
    return parser.parse_args()


def sync_main():
    asyncio.run(main(parse_args()))


if __name__ == "__main__":
    sync_main()
