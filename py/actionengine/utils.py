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

"""Basic utils for pythonic ActionEngine Light."""

import asyncio
from typing import Any
from typing import Coroutine
from typing import TypeVar

from actionengine import data

T = TypeVar("T")
Instance = TypeVar("Instance")

Chunk = data.Chunk

PENDING_ASYNC_TASKS: set[asyncio.Task] = set()


def is_null_chunk(chunk: Chunk) -> bool:
    if chunk is None:
        return False
    return (
        not chunk.data and chunk.metadata.mimetype == "application/octet-stream"
    )


def wrap_pybind_object(cls: type[T], impl: T):
    wrapped = cls.__new__(cls)
    super(cls, wrapped).__init__(impl)
    if hasattr(wrapped, "_add_python_specific_attributes"):
        wrapped._add_python_specific_attributes()  # pylint: disable=protected-access
    return wrapped


def schedule_global_task(coro: Coroutine[Any, Any, Any]) -> asyncio.Task:
    """Adds the given task to the set of top-level tasks.

    The task runs in the background and the caller may choose not to wait for it
    to complete, nor receive any results or errors. The caller does not have to
    keep a reference to the task, but might need to keep references to any
    objects that the task holds onto.

    Args:
      coro: The coroutine to run.

    Returns:
      The task that runs the coroutine in the background.
    """
    task = asyncio.create_task(coro)
    PENDING_ASYNC_TASKS.add(task)
    task.add_done_callback(lambda _: PENDING_ASYNC_TASKS.remove(task))
    return task
