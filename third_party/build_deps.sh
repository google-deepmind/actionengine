set -e

mkdir -p build_deps

third_party_root=$(pwd)
cc_standard=20
nproc=8

python_exec_prefix=$(python -c "import sys; print(sys.exec_prefix)")

abseil_install_dir="${third_party_root}/build_deps/abseil-cpp"
boost_install_dir="${third_party_root}/build_deps/boost"
cppitertools_install_dir="${third_party_root}/build_deps/cppitertools"
googletest_install_dir="${third_party_root}/build_deps/googletest"
pybind11_install_dir="${third_party_root}/build_deps/pybind11"
pybind11_abseil_install_dir="${third_party_root}/build_deps/pybind11_abseil"

# Google Test
mkdir -p build_deps/googletest
cd googletest
mkdir -p build
cd build
cmake \
  -DCMAKE_CXX_STANDARD="${cc_standard}" \
  -DCMAKE_INSTALL_PREFIX="${googletest_install_dir}" \
  -G "Ninja" \
  ..
rm -rf "${googletest_install_dir}"
cmake --build . --parallel "${nproc}" --target install
cd "${third_party_root}"

# Abseil
mkdir -p build_deps/abseil-cpp
cd abseil-cpp
mkdir -p build
cd build
cmake \
  -DABSL_BUILD_TESTING=ON \
  -DABSL_RUN_TESTS=OFF \
  -DABSL_LOCAL_GOOGLETEST_DIR="${third_party_root}/googletest" \
  -DABSL_FIND_GOOGLETEST=ON \
  -DCMAKE_PREFIX_PATH="${googletest_install_dir}" \
  -DCMAKE_CXX_STANDARD="${cc_standard}" \
  -DCMAKE_INSTALL_PREFIX="${abseil_install_dir}" \
  -G "Ninja" \
  ..
rm -rf "${abseil_install_dir}"
cmake --build . --parallel "${nproc}" --target install
cd "${third_party_root}"

# Boost
mkdir -p build_deps/boost
cd boost
mkdir -p build
cd build
cmake \
  -DCMAKE_CXX_STANDARD="${cc_standard}" \
  -DCMAKE_INSTALL_PREFIX="${boost_install_dir}" \
  -DBOOST_INCLUDE_LIBRARIES="fiber;intrusive;beast" \
  -DBOOST_ENABLE_CMAKE=ON \
  -G "Ninja" \
  ..
rm -rf "${boost_install_dir}"
cmake --build . --parallel "${nproc}" --target install
cd "${third_party_root}"

# Pybind11
mkdir -p build_deps/pybind11
cd pybind11
mkdir -p build
cd build
cmake \
  -DCMAKE_CXX_STANDARD="${cc_standard}" \
  -DCMAKE_INSTALL_PREFIX="${pybind11_install_dir}" \
  -DPython_ROOT_DIR="${python_exec_prefix}" \
  -DPython3_ROOT_DIR="${python_exec_prefix}" \
  -G "Ninja" \
  ..
rm -rf "${pybind11_install_dir}"
cmake --build . --parallel "${nproc}" --target install
cd "${third_party_root}"

# cppitertools
mkdir -p build_deps/cppitertools
cd cppitertools
mkdir -p __build
cd __build
cmake \
  -DCMAKE_CXX_STANDARD="${cc_standard}" \
  -DCMAKE_INSTALL_PREFIX="${cppitertools_install_dir}" \
  -G "Ninja" \
  ..
rm -rf "${cppitertools_install_dir}"
cmake --build . --parallel "${nproc}" --target install
cd "${third_party_root}"

# ZMQ
mkdir -p build_deps/libzmq && cd libzmq
mkdir -p build && cd build
cmake \
  -DCMAKE_CXX_STANDARD="${cc_standard}" \
  -DCMAKE_INSTALL_PREFIX="${third_party_root}/build_deps/libzmq" \
  -G "Ninja" \
  ..
cmake --build . --parallel "${nproc}" --target install
cd "${third_party_root}"

mkdir -p build_deps/cppzmq && cd cppzmq
mkdir -p build && cd build
cmake \
  -DCMAKE_CXX_STANDARD="${cc_standard}" \
  -DCMAKE_PREFIX_PATH="${third_party_root}/build_deps/libzmq" \
  -DCMAKE_INSTALL_PREFIX="${third_party_root}/build_deps/cppzmq" \
  -DCPPZMQ_BUILD_TESTS=OFF \
  -G "Ninja" \
  ..
cmake --build . --parallel "${nproc}" --target install
cd "${third_party_root}"