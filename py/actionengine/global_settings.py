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

"""act util to use global settings that will apply by default if not overridden."""

import dataclasses
from typing import TypeVar

T = TypeVar("T")


@dataclasses.dataclass()
class EGLTSettings:
    """Context for act."""

    readers_deserialise_automatically: bool = False
    readers_read_in_order: bool = True
    readers_remove_read_chunks: bool = True
    readers_buffer_size: int = -1
    readers_timeout: float = -1.0


_GLOBAL_SETTINGS = EGLTSettings()


def get_global_act_settings() -> EGLTSettings:
    """Returns the global settings for act."""
    return _GLOBAL_SETTINGS


def global_setting_if_none(value: T | None, setting: str) -> T:
    """Returns the globally set value if the value is None, otherwise returns the value."""
    if value is None:
        return getattr(_GLOBAL_SETTINGS, setting)
    return value
