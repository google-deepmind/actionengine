"""Websocket support for Evergreen."""

from evergreen.evergreen_pybind11 import \
  websockets as websockets_pybind11

make_websocket_evergreen_stream = websockets_pybind11.make_websocket_evergreen_stream
WebsocketEvergreenServer = websockets_pybind11.WebsocketEvergreenServer
