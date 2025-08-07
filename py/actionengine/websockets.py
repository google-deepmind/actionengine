"""Websocket support for ActionEngine."""

from actionengine.actionengine_pybind11 import websockets as websockets_pybind11

make_websocket_stream = websockets_pybind11.make_websocket_stream
WebsocketServer = websockets_pybind11.WebsocketServer
