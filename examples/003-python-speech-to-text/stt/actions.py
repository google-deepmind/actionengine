import asyncio

import evergreen

from .serialisation import BYTEARRAY_MIMETYPE
from .server import STTServer


def has_stop_command(text: str) -> bool:
  return (
      len(text) <= 5
      and (
          text.lower().startswith("stop")
          or text.lower().startswith("exit")
      )
  )


def stream_text_output_to_node(node: evergreen.AsyncNode):
  server = STTServer.instance()

  try:
    while True:
      text = server.get_text()
      node.put(text)

      if has_stop_command(text):
        print("Stop command received, stopping output stream.", flush=True)
        break

  finally:
    node.finalize()


async def run_speech_to_text(action: evergreen.Action):
  print("Running speech_to_text action", flush=True)
  server = STTServer.instance()

  stream_output_task = asyncio.create_task(
      asyncio.to_thread(stream_text_output_to_node, action.get_output("text"))
  )

  async for audio_chunk in action.get_input("speech"):
    server.feed_chunk(audio_chunk)

  await stream_output_task


def make_action_registry():
  registry = evergreen.ActionRegistry()
  registry.register(
      "speech_to_text",
      evergreen.ActionDefinition(
          name="speech_to_text",
          inputs=[
              evergreen.ActionNode(name="speech", mimetype=BYTEARRAY_MIMETYPE)
          ],
          outputs=[evergreen.ActionNode(name="text", mimetype="text/plain")],
      ),
      run_speech_to_text,
  )
  return registry
