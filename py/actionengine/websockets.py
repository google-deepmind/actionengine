"""Websocket support for ActionEngine."""

from actionengine.actionengine_pybind11 import websockets as websockets_pybind11

make_websocket_actionengine_stream = (
    websockets_pybind11.make_websocket_actionengine_stream
)
WebsocketActionEngineServer = websockets_pybind11.WebsocketActionEngineServer
