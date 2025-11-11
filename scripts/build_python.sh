#!/usr/bin/env bash

set -e

function is_true() {
  local lower_valued
  lower_valued=$(echo "$1" | tr "[:upper:]" "[:lower:]")
  if [[ "$lower_valued" == "true" || "$lower_valued" == "yes" || "$lower_valued" == "1" || "$lower_valued" == "on" ]]; then
    echo "1"
  else
    echo "0"
  fi
}

nproc=8

repo_root=$(realpath "$(dirname "$(realpath "$0")")/..")
cd "$repo_root" || exit 1

if [[ -z "$CC" ]]; then
  cc="clang"
else
  cc="$CC"
fi

if [[ -z "$CXX" ]]; then
  cxx="clang++"
else
  cxx="$CXX"
fi

echo "Building project..."
cd build
CC=${cc} CXX=${cxx} cmake --build . --parallel "${nproc}" --target actionengine_pybind11
CC=${cc} CXX=${cxx} cmake --build . --parallel "${nproc}" --target actProto
CC=${cc} CXX=${cxx} cmake --build . --parallel "${nproc}" --target pybind11_abseil_status_module

echo "Moving compiled files to Python code directory..."
cd "$repo_root"
for f in build/src/actionengine_pybind11*.so; do
    echo "Copying $f to py/actionengine/_C${f##*/actionengine_pybind11}"
    dst_path="py/actionengine/_C${f##*/actionengine_pybind11}"
    cp -f "$f" "$dst_path"
    # if env variable ACTIONENGINE_MACOS_USE_RPATH is set to 1,
    # we need to adjust the dylib load paths to use rpath on macOS
    use_rpath_env="0"
    if [[ -n "${ACTIONENGINE_MACOS_USE_RPATH}" ]]; then
        use_rpath_env="${ACTIONENGINE_MACOS_USE_RPATH}"
    fi
    if [[ "$(uname)" == "Darwin" && $(is_true "${use_rpath_env}") == "1" ]]; then
        echo "Adjusting dylib paths for $dst_path"
        python_version=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
        python "$repo_root/build_py/macos_patch_dylib_path.py" "$dst_path"  "libpython${python_version}.dylib" "@rpath" --ignore-errors
        python "$repo_root/build_py/macos_patch_dylib_path.py" "$dst_path"  "libpython3.dylib" "@rpath" --ignore-errors
        python "$repo_root/build_py/macos_patch_dylib_path.py" "$dst_path"  "libssl.3.dylib" "@rpath" --ignore-errors
        python "$repo_root/build_py/macos_patch_dylib_path.py" "$dst_path"  "libssl.dylib" "@rpath" --ignore-errors
        python "$repo_root/build_py/macos_patch_dylib_path.py" "$dst_path"  "libcrypto.3.dylib" "@rpath" --ignore-errors
        python "$repo_root/build_py/macos_patch_dylib_path.py" "$dst_path"  "libcrypto.dylib" "@rpath" --ignore-errors
    fi
done
for f in build/src/actionengine/proto/*_pb2.py; do
    echo "Copying $f to py/actionengine/proto/${f##*/}"
    cp -f "$f" "py/actionengine/proto/${f##*/}"
done
for f in build/src/pybind11_abseil_status_module/pybind11_abseil_status_module.*.so; do
    echo "Copying $f to py/actionengine/status.${f##*/pybind11_abseil_status_module.}"
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