# Serving a speech-to-text model with actions

This example demonstrates how you can serve a speech-to-text experience by
wrapping an external model with an ActionEngine action. To run this example,
you will need to have [RealtimeSTT](https://github.com/KoljaB/RealtimeSTT)
installed:

```bash
pip install git+https://github.com/KoljaB/RealtimeSTT
```

## Server

Run the server with the following command:

```bash
python run_server.py
```

## Client

Run the client with the following command:

```bash
python run_client.py
```

The client will open a WebSocket connection to the server, call the
`speech_to_text` action defined in `stt/actions.py`, and start sending
audio data to the input node `speech` of the action. The server will process
the audio data and return the transcribed text by streaming it to the
`text` output node of the action. The client will print the transcribed text
as it is received.

To stop the action, just __say__
> stop

or
> exit