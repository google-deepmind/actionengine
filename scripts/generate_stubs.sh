#!/usr/bin/env bash

set -e

repo_root=$(realpath "$(dirname "$(realpath "$0")")/..")
cd "$repo_root" || exit 1

cd "${repo_root}/py"

echo "Generating .pyi stubs."
PYTHONPATH=$(pwd) pybind11-stubgen --ignore-invalid-expressions ".*" -o . actionengine._C
echo "Done."
