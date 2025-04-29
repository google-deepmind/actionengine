import asyncio

import evergreen
from RealtimeSTT import AudioToTextRecorder

from stt.actions import has_stop_command
from stt.actions import make_action_registry
from stt.serialisation import register_stt_serialisers


def setup_action_engine():
  register_stt_serialisers()

  settings = evergreen.get_global_eglt_settings()
  settings.readers_deserialise_automatically = True
  settings.readers_read_in_order = True
  settings.readers_remove_read_chunks = True


async def main():
  setup_action_engine()

  action_registry = make_action_registry()
  node_map = evergreen.NodeMap()
  print("Connecting to server.")
  stream = evergreen.websockets.make_websocket_evergreen_stream(
      "localhost", "/", 20000
  )

  print("Connected, starting session.")
  session = evergreen.Session(node_map, action_registry)
  session.dispatch_from(stream)

  action = action_registry.make_action(
      "speech_to_text", node_map=node_map, stream=stream, session=session
  )

  print("Initialising audio recorder, please wait.")
  recorder = AudioToTextRecorder(
      spinner=True,
      on_recorded_chunk=lambda audio: action.get_input("speech").put(audio),
  )

  print("Recording started, you can start speaking.")
  await action.call()

  try:
    async for text in action.get_output("text"):
      print(text)
      if has_stop_command(text):
        print("Stop command received, stopping recording.")
        break
  finally:
    recorder.shutdown()
    print("Stopped recording.")

    await action.get_input("speech").finalize()
    await asyncio.sleep(0.1)
    print("Finalised speech stream.")

    session.stop_dispatching_from(stream)
    stream.close()
    print("Closed the client-server stream.")


if __name__ == "__main__":
  asyncio.run(main())
