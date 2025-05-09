set -e

cc_standard=20

repo_root=$(pwd)

# skip dependencies if specified
if [[ "$1" == "--skip-deps" ]]; then
  echo "Skipping third-party dependencies build..."
else
  echo "Updating submodules..."
  git submodule update --init --recursive
  echo "Building third-party dependencies..."
  cd third_party
  CC=clang CXX=clang++ ./build_deps.sh
  cd "$repo_root"
fi

mkdir -p build
cd build
echo "Configuring build..."
CC=clang CXX=clang++ cmake \
  -DCMAKE_CXX_STANDARD="${cc_standard}" \
  -DCMAKE_INSTALL_PREFIX="${repo_root}/install" \
  -DCMAKE_BUILD_TYPE=Debug \
  -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
  -GNinja \
  ..
