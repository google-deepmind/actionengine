import asyncio

import evergreen

from .serialisation import BYTEARRAY_MIMETYPE
from .server import STTServer


def make_output_callback(node: evergreen.AsyncNode):
  def callback(text: str):
    node.put_text(text)

  return callback


def fill_text_output(server, text_output):
  callback = make_output_callback(text_output)
  while True:
    server.get_text(callback)


async def run_speech_to_text(action: evergreen.Action):
  print("Running speech_to_text action", flush=True)
  server = STTServer.instance()

  text_output = action.get_output("text")
  output_task = asyncio.create_task(
      asyncio.to_thread(fill_text_output, server, text_output)
  )

  speech_input = action.get_input("speech")

  try:
    async for audio_chunk in speech_input:
      server.feed_chunk(audio_chunk)
  finally:
    await text_output.finalize()

  output_task.cancel()
  await output_task


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
