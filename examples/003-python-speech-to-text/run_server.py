import asyncio

import evergreen

from stt.actions import make_action_registry
from stt.model_server import STTModelServer
from stt.serialisation import register_stt_serialisers


def setup_action_engine():
  register_stt_serialisers()

  settings = evergreen.get_global_eglt_settings()
  settings.readers_deserialise_automatically = True
  settings.readers_read_in_order = True
  settings.readers_remove_read_chunks = True


async def main():
  setup_action_engine()

  print("Starting speech-to-text API backend.")
  STTModelServer.instance()

  action_registry = make_action_registry()
  service = evergreen.Service(action_registry)
  server = evergreen.websockets.WebsocketEvergreenServer(service)

  print("Starting Action Engine server.")
  server.run()
  await asyncio.to_thread(server.join)


def sync_main():
  return asyncio.run(main())


if __name__ == "__main__":
  sync_main()
