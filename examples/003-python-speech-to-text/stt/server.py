import numpy as np
from RealtimeSTT import AudioToTextRecorder


class STTServer:
  def __init__(self):
    self._recorder = AudioToTextRecorder(
        post_speech_silence_duration=0.4,
        ensure_sentence_starting_uppercase=False,
        ensure_sentence_ends_with_period=False,
        use_microphone=False,
        spinner=False,
    )

  @staticmethod
  def instance():
    if not hasattr(STTServer, "_instance"):
      STTServer._instance = STTServer()

    return STTServer._instance

  def feed_chunk(self, chunk: np.ndarray):
    self._recorder.feed_audio(chunk)

  def get_text(self, callback=None):
    return self._recorder.text(callback)
