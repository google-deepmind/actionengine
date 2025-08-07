set -e

nproc=8

repo_root=$(pwd)

echo "Building project..."
cd build
CC=clang CXX=clang++ cmake --build . --parallel "${nproc}" --target actionengine_pybind11
CC=clang CXX=clang++ cmake --build . --parallel "${nproc}" --target pybind11_abseil

echo "Moving compiled files to Python code directory..."
cd "$repo_root"
mv build/src/actionengine_pybind11*.so py/actionengine/
mv build/src/pybind11_abseil_module/pybind11_abseil*.so py/actionengine/
rm -rf install

if [[ "$1" == "--only-rebuild-pybind11" ]]; then
  echo "Skipping Python requirements."
else
  echo "Installing requirements and Python package and cleaning up."
  pip3 install -r py/requirements.txt
  pip3 install --force-reinstall -e ./py
fi

echo "Validating installation."
python3 -c "import actionengine; print('ActionEngine imports successfully!')"
