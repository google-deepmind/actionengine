#!/usr/bin/env bash

set -e

nproc=8

repo_root=$(realpath "$(dirname "$(realpath "$0")")/..")
cd "$repo_root" || exit 1

echo "Building project..."
cd build
CC=clang CXX=clang++ cmake --build . --parallel "${nproc}" --target actionengine_pybind11
CC=clang CXX=clang++ cmake --build . --parallel "${nproc}" --target actProto
CC=clang CXX=clang++ cmake --build . --parallel "${nproc}" --target pybind11_abseil_status_module

echo "Moving compiled files to Python code directory..."
cd "$repo_root"
for f in build/src/actionengine_pybind11*.so; do
    cp -f "$f" "py/actionengine/_C${f##*/actionengine_pybind11}"
done
for f in build/src/actionengine/proto/*_pb2.py; do
    cp -f "$f" "py/actionengine/proto/${f##*/}"
done
for f in build/src/pybind11_abseil_status_module/pybind11_abseil_status_module.*.so; do
    cp -f "$f" "py/actionengine/status.${f##*/pybind11_abseil_status_module.}"
done
rm -rf install

if [[ "$1" == "--only-rebuild-pybind11" ]]; then
  echo "Skipping Python requirements."
else
  echo "Installing requirements and Python package and cleaning up."
  pip3 install -r py/requirements.txt
  pip3 install --force-reinstall -e .
fi
