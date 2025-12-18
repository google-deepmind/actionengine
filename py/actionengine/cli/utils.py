import asyncio

import actionengine


async def sleep_forever():
    while True:
        await asyncio.sleep(1)


def setup_action_engine():
    settings = actionengine.get_global_act_settings()
    settings.readers_deserialise_automatically = True
    settings.readers_read_in_order = True
    settings.readers_remove_read_chunks = True
