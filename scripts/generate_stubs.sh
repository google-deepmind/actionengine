#!/usr/bin/env bash

set -e


# if not given a path as argument, assume the script is located in scripts/ and
# the repo root is one level up
if [[ -n "$1" ]]; then
  workdir=$(realpath "$1")
else
  workdir="$(realpath "$(dirname "$(realpath "$0")")/..")/py"
fi

cd "${workdir}" || exit 1

echo "Generating .pyi stubs at ${workdir}..."
PYTHONPATH=${workdir}:$PYTHONPATH pybind11-stubgen --ignore-invalid-expressions ".*" -o . actionengine._C
echo "Done."
