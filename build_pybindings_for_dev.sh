set -e

nproc=8
cc_standard=20

repo_root=$(pwd)

echo "Updating submodules..."
git submodule update --init --recursive

echo "Building third-party dependencies..."
cd third_party
./build_deps.sh
cd "$repo_root"

mkdir -p build
cd build
echo "Configuring build..."
cmake \
  -DCMAKE_CXX_STANDARD="${cc_standard}" \
  -DCMAKE_INSTALL_PREFIX="${repo_root}/install" \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
  -GNinja \
  ..

echo "Building project..."
cmake --build . --parallel "${nproc}" --target install

echo "Moving compiled files to Python code directory..."
cd "$repo_root"
mv install/lib/eglt/evergreen_pybind11*.so py/evergreen/
rm -rf install

echo "Installing and cleaning up..."
pip install -r py/requirements.txt
pip install --force-reinstall -e py

echo "Validating installation..."
python -c "import evergreen; print('Evergreen imports successfully!')"