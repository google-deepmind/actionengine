set -e

nproc=8

repo_root=$(pwd)

echo "Building project..."
cd build
CC=clang CXX=clang++ cmake --build . --parallel "${nproc}" --target evergreen_pybind11

echo "Moving compiled files to Python code directory..."
cd "$repo_root"
mv build/src/evergreen_pybind11*.so py/evergreen/
rm -rf install

if [[ "$1" == "--only-rebuild-pybind11" ]]; then
  echo "Skipping Python requirements."
else
  echo "Installing requirements and Python package and cleaning up."
  pip install -r py/requirements.txt
  pip install --force-reinstall -e ./py
fi

echo "Validating installation."
python -c "import evergreen; print('Evergreen imports successfully!')"