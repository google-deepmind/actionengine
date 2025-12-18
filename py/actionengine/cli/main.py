import argparse
import asyncio
import logging

from actionengine.cli.auth import register_command
from actionengine.cli.serve import serve_command
from actionengine.cli.utils import setup_action_engine


API_ROOT = "https://actionengine.dev/api"

logger = logging.getLogger(__name__)


async def sleep_forever():
    while True:
        await asyncio.sleep(1)


async def main(args: argparse.Namespace):
    logging.basicConfig(level=logging.INFO)

    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)-8s %(message)s")
    )
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    setup_action_engine()

    if args.command == "serve":
        return await serve_command(args)
    elif args.command == "register":
        return await register_command()
    elif args.command == "create-api-key":
        raise NotImplementedError(
            "create-api-key command is not implemented yet."
        )

    raise ValueError(f"Unknown command: {args.command}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Health check for Action Engine host."
    )
    parser.add_argument(
        "command",
        help="The command to execute.",
        choices=["serve", "register", "create-api-key"],
    )
    parser.add_argument(
        "--api-key",
        type=str,
        default=None,
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
