import numpy as np
from RealtimeSTT import AudioToTextRecorder


class STTModelServer:
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
    if not hasattr(STTModelServer, "_instance"):
      STTModelServer._instance = STTModelServer()

    return STTModelServer._instance

  def feed_audio_chunk(self, chunk: np.ndarray):
    self._recorder.feed_audio(chunk)

  def wait_for_transcription_piece(self, callback=None):
    return self._recorder.text(callback)
