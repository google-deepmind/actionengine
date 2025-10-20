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
import os
import pathlib

from setuptools import find_packages
from setuptools import setup

_CURRENT_DIR = pathlib.Path(__file__).parent.resolve()
_PREBUILT_EXTENSIONS = [
    str(path.relative_to(_CURRENT_DIR))
    for path in (pathlib.Path(_CURRENT_DIR) / "py").glob("**/*.so")
]
_PROTO = [
    str(path.relative_to(_CURRENT_DIR))
    for path in (pathlib.Path(_CURRENT_DIR) / "py").glob("proto/*")
]
print(f"Prebuilt extensions found: {_PREBUILT_EXTENSIONS}")

setup(
    name="actionengine",
    version="0.1.2",
    author="Google LLC",
    author_email="helenapankov@google.com",
    license="Apache License, Version 2.0",
    url="https://github.com/google-deepmind/actionengine-cpp",
    packages=find_packages("py"),
    package_data={
        "": _PREBUILT_EXTENSIONS + _PROTO,
    },
    include_package_data=True,
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 3.12",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
    ],
)
