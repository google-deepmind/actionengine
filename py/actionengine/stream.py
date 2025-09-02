# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""A Pythonic wrapper for the raw pybind11 Session bindings."""

import asyncio

from actionengine import data
from actionengine import utils
from actionengine._C import service as service_pybind11


class WireStream(service_pybind11.WireStream):
    """A Pythonic wrapper for the raw pybind11 WireStream bindings."""

    async def receive(self) -> data.WireMessage | None:
        """Receives a message from the stream."""
        return await asyncio.to_thread(
            super().receive
        )  # pytype: disable=attribute-error

    async def send(self, message: data.WireMessage) -> None:
        """Sends a message to the stream."""
        await asyncio.to_thread(
            super().send, message
        )  # pytype: disable=attribute-error
