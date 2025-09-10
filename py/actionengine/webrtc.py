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

from actionengine import _C

make_webrtc_stream = _C.webrtc.make_webrtc_stream

# Simply reexport "data" classes
TurnServer = _C.webrtc.TurnServer
RtcConfig = _C.webrtc.RtcConfig


class WebRtcServer(_C.webrtc.WebRtcServer):
    pass
