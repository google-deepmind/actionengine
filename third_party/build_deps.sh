mkdir -p build_deps

third_party_root=$(pwd)
cc_standard=20
nproc=8

mkdir -p build_deps/abseil-cpp
cd abseil-cpp || exit 1
mkdir -p build
cd build || exit 1
cmake \
  -DABSL_BUILD_TESTING=ON \
  -DABSL_USE_GOOGLETEST_HEAD=ON \
  -DCMAKE_CXX_STANDARD="${cc_standard}" \
  -DCMAKE_INSTALL_PREFIX="${third_party_root}/build_deps/abseil-cpp" \
  -G "Ninja" \
  .. || exit 1
cmake --build . --parallel "${nproc}" --target install || exit 1
cd "${third_party_root}" || exit 1

mkdir -p build_deps/googletest
cd googletest || exit 1
mkdir -p build
cd build || exit 1
cmake \
  -DCMAKE_PREFIX_PATH="${third_party_root}/build_deps/abseil-cpp" \
  -DCMAKE_CXX_STANDARD="${cc_standard}" \
  -DCMAKE_INSTALL_PREFIX="${third_party_root}/build_deps/googletest" \
  -G "Ninja" \
  .. || exit 1
cmake --build . --parallel "${nproc}" --target install || exit 1
cd "${third_party_root}" || exit 1

mkdir -p build_deps/boost
cd boost || exit 1
mkdir -p build
cd build || exit 1
cmake \
  -DCMAKE_CXX_STANDARD="${cc_standard}" \
  -DCMAKE_INSTALL_PREFIX="${third_party_root}/build_deps/boost" \
  -DBOOST_INCLUDE_LIBRARIES="fiber" \
  -DBOOST_ENABLE_CMAKE=ON \
  -G "Ninja" \
  .. || exit 1
cmake --build . --parallel "${nproc}" --target install || exit 1
cd "${third_party_root}" || exit 1

mkdir -p build_deps/poco
cd poco || exit 1
mkdir -p build
cd build || exit 1
cmake \
  -DCMAKE_CXX_STANDARD="${cc_standard}" \
  -DCMAKE_INSTALL_PREFIX="${third_party_root}/build_deps/poco" \
  -G "Ninja" \
  .. || exit 1
cmake --build . --parallel "${nproc}" --target install || exit 1
cd "${third_party_root}" || exit 1

mkdir -p build_deps/pybind11
cd pybind11 || exit 1
mkdir -p build
cd build || exit 1
cmake \
  -DCMAKE_CXX_STANDARD="${cc_standard}" \
  -DCMAKE_INSTALL_PREFIX="${third_party_root}/build_deps/pybind11" \
  -DPython_ROOT_DIR="$(python -c "import sys; print(sys.exec_prefix)")" \
  -G "Ninja" \
  .. || exit 1
cmake --build . --parallel "${nproc}" --target install || exit 1
cd "${third_party_root}" || exit 1

