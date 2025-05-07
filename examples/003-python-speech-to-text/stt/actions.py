import asyncio

import evergreen

from .model_server import STTModelServer
from .serialisation import BOOL_MIMETYPE
from .serialisation import BYTEARRAY_MIMETYPE


def has_stop_command(text: str) -> bool:
  return (
      len(text) <= 5
      and (
          text.lower().startswith("stop")
          or text.lower().startswith("exit")
      )
  )


def stream_text_output(action: evergreen.Action,
    model_server: STTModelServer):
  output_node = action.get_output("text")
  ready_node = action.get_output("ready")

  ready_node.put(True)
  ready_node.finalize()

  try:
    while True:
      transcription = model_server.wait_for_transcription_piece()
      output_node.put(transcription)

      if has_stop_command(transcription):
        print("Stop command received, stopping output stream.", flush=True)
        break

  finally:
    output_node.finalize()


async def run_speech_to_text(action: evergreen.Action):
  print("Running speech_to_text action", flush=True)
  model_server = STTModelServer()

  stream_output_task = asyncio.create_task(
      asyncio.to_thread(stream_text_output, action, model_server)
  )

  async for audio_chunk in action.get_input("speech"):
    model_server.feed_audio_chunk(audio_chunk)

  await stream_output_task


def make_action_registry():
  registry = evergreen.ActionRegistry()
  registry.register(
      "speech_to_text",
      evergreen.ActionSchema(
          name="speech_to_text",
          inputs=[("speech", BYTEARRAY_MIMETYPE)],
          outputs=[("text", "text/plain"), ("ready", BOOL_MIMETYPE)],
      ),
      run_speech_to_text,
  )
  return registry
