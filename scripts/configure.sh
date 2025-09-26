#!/usr/bin/env bash

set -e

if [[ -z "$CMAKE_BUILD_TYPE" ]]; then
  cmake_build_type="Debug"
else
  cmake_build_type="$CMAKE_BUILD_TYPE"
fi

cc_standard=20

repo_root=$(dirname "$(realpath "$0")")/..
deps_dir="${repo_root}/third_party"
build_dir="${repo_root}/build"

# skip dependencies if specified
if [[ "$1" == "--skip-deps" ]]; then
  echo "Skipping third-party dependencies build..."
else
  echo "Updating submodules..."
  (cd "${repo_root}" && git submodule update --init --recursive)
  echo "Building third-party dependencies..."
  (cd "${deps_dir}" && CC=clang CXX=clang++ ./build_deps.sh)
fi


echo "Configuring build..."
(mkdir -p "${build_dir}" \
  && cd "${build_dir}" \
  && CC=clang CXX=clang++ cmake \
  -DCMAKE_CXX_STANDARD="${cc_standard}" \
  -DCMAKE_INSTALL_PREFIX="${repo_root}/install" \
  -DCMAKE_BUILD_TYPE="${cmake_build_type}" \
  -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
  -GNinja \
  ..)
