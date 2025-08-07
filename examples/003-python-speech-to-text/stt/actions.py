import asyncio

import actionengine

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


async def stream_text_output(action: actionengine.Action,
    model_server: STTModelServer):
  output_node = action.get_output("text")
  ready_node = action.get_output("ready")

  await ready_node.put(True)
  await ready_node.finalize()

  try:
    while True:
      transcription = await asyncio.to_thread(
          model_server.wait_for_transcription_piece)
      await output_node.put(transcription)

      if has_stop_command(transcription):
        print("Stop command received, stopping output stream.", flush=True)
        break

  except asyncio.CancelledError:
    print("Output stream cancelled.", flush=True)

  finally:
    print("Finalising output stream.", flush=True)
    await output_node.finalize()


async def run_speech_to_text(action: actionengine.Action):
  print("Running speech_to_text action", flush=True)
  model_server = STTModelServer()

  stream_output_task = asyncio.create_task(
      stream_text_output(action, model_server)
  )

  async for audio_chunk in action.get_input("speech"):
    model_server.feed_audio_chunk(audio_chunk)

  print("Finalising speech input stream.", flush=True)
  model_server._recorder.shutdown()
  stream_output_task.cancel()
  await stream_output_task


def make_action_registry():
  registry = actionengine.ActionRegistry()
  registry.register(
      "speech_to_text",
      actionengine.ActionSchema(
          name="speech_to_text",
          inputs=[("speech", BYTEARRAY_MIMETYPE)],
          outputs=[("text", "text/plain"), ("ready", BOOL_MIMETYPE)],
      ),
      run_speech_to_text,
  )
  return registry
