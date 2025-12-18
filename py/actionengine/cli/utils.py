import asyncio
from os import PathLike
from pathlib import Path

import actionengine


API_ROOT = "https://actionengine.dev/api"


def load_username_and_api_key_from_file(
    path: PathLike | None = None,
) -> tuple[str, str]:
    path = Path(path or Path.home() / ".actionengine" / "credentials")
    with path.open("r", encoding="utf-8") as f:
        credentials = f.read()

    username, api_key = credentials.strip().split(":", 1)
    if " " in username or " " in api_key:
        raise ValueError(
            "Invalid credentials file format: username and API key "
            "must not contain spaces."
        )
    return username, api_key


async def sleep_forever():
    while True:
        await asyncio.sleep(1)


def setup_action_engine():
    settings = actionengine.get_global_act_settings()
    settings.readers_deserialise_automatically = True
    settings.readers_read_in_order = True
    settings.readers_remove_read_chunks = True
