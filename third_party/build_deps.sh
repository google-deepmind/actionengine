#/usr/bin/env bash

set -e

isset()   { [[ $(eval echo "\${${1}+1}") ]]; }
isunset() { ! isset "$1"; }

mkdir -p build_deps

third_party_root=$(pwd)
cc_standard=20
if isunset parallelism; then
  if ! command -v nproc &> /dev/null; then
    parallelism=6
  else
    parallelism=$(nproc)
  fi
fi
if isunset build_folder_name; then
  build_folder_name="build"
fi

python_exec_prefix=$(python3 -c "import sys; print(sys.exec_prefix)")

abseil_install_dir="${third_party_root}/build_deps/abseil-cpp"
boost_install_dir="${third_party_root}/build_deps/boost"
cppitertools_install_dir="${third_party_root}/build_deps/cppitertools"
googletest_install_dir="${third_party_root}/build_deps/googletest"
libdatachannel_install_dir="${third_party_root}/build_deps/libdatachannel"
pybind11_install_dir="${third_party_root}/build_deps/pybind11"
pybind11_abseil_install_dir="${third_party_root}/build_deps/pybind11_abseil"
hiredis_install_dir="${third_party_root}/build_deps/hiredis"

# Google Test
mkdir -p build_deps/googletest
cd googletest
mkdir -p "${build_folder_name}"
cd "${build_folder_name}"
cmake \
  -DCMAKE_CXX_STANDARD="${cc_standard}" \
  -DCMAKE_INSTALL_PREFIX="${googletest_install_dir}" \
  -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
  -G "Ninja" \
  ..
rm -rf "${googletest_install_dir}"
cmake --build . --parallel "${parallelism}" --target install
cd "${third_party_root}"

# Abseil
mkdir -p build_deps/abseil-cpp
cd abseil-cpp
mkdir -p "${build_folder_name}"
cd "${build_folder_name}"
cmake \
  -DABSL_BUILD_TESTING=OFF \
  -DABSL_BUILD_TEST_HELPERS=ON \
  -DABSL_RUN_TESTS=OFF \
  -DABSL_LOCAL_GOOGLETEST_DIR="${third_party_root}/googletest" \
  -DABSL_FIND_GOOGLETEST=ON \
  -DCMAKE_PREFIX_PATH="${googletest_install_dir}" \
  -DCMAKE_CXX_STANDARD="${cc_standard}" \
  -DCMAKE_INSTALL_PREFIX="${abseil_install_dir}" \
  -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
  -G "Ninja" \
  ..
rm -rf "${abseil_install_dir}"
cmake --build . --parallel "${parallelism}" --target install
cd "${third_party_root}"

# Pybind11
mkdir -p build_deps/pybind11
cd pybind11
mkdir -p "${build_folder_name}"
cd "${build_folder_name}"
cmake \
  -DCMAKE_CXX_STANDARD="${cc_standard}" \
  -DCMAKE_INSTALL_PREFIX="${pybind11_install_dir}" \
  -DPython_ROOT_DIR="${python_exec_prefix}" \
  -DPython3_ROOT_DIR="${python_exec_prefix}" \
  -DPYBIND11_TEST=OFF \
  -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
  -G "Ninja" \
  ..
rm -rf "${pybind11_install_dir}"
cmake --build . --parallel "${parallelism}" --target install
cd "${third_party_root}"

# cppitertools
mkdir -p build_deps/cppitertools
cd cppitertools
mkdir -p "__${build_folder_name}"
cd "__${build_folder_name}"
cmake \
  -DCMAKE_CXX_STANDARD="${cc_standard}" \
  -DCMAKE_INSTALL_PREFIX="${cppitertools_install_dir}" \
  -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
  -G "Ninja" \
  ..
rm -rf "${cppitertools_install_dir}"
cmake --build . --parallel "${parallelism}" --target install
cd "${third_party_root}"

# ZMQ
mkdir -p build_deps/libzmq && cd libzmq
mkdir -p "${build_folder_name}" && cd "${build_folder_name}"
cmake \
  -DCMAKE_CXX_STANDARD="${cc_standard}" \
  -DCMAKE_INSTALL_PREFIX="${third_party_root}/build_deps/libzmq" \
  -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
  -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
  -G "Ninja" \
  ..
cmake --build . --parallel "${parallelism}" --target install
cd "${third_party_root}"

mkdir -p build_deps/cppzmq && cd cppzmq
mkdir -p "${build_folder_name}" && cd "${build_folder_name}"
cmake \
  -DCMAKE_CXX_STANDARD="${cc_standard}" \
  -DCMAKE_PREFIX_PATH="${third_party_root}/build_deps/libzmq" \
  -DCMAKE_INSTALL_PREFIX="${third_party_root}/build_deps/cppzmq" \
  -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
  -DCPPZMQ_BUILD_TESTS=OFF \
  -G "Ninja" \
  ..
cmake --build . --parallel "${parallelism}" --target install
cd "${third_party_root}"

# libdatachannel
mkdir -p build_deps/libdatachannel && cd libdatachannel
mkdir -p "${build_folder_name}" && cd "${build_folder_name}"
cmake \
  -DUSE_GNUTLS=0 \
  -DUSE_NICE=0 \
  -DNO_MEDIA=1 \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_CXX_STANDARD="${cc_standard}" \
  -DCMAKE_INSTALL_PREFIX="${libdatachannel_install_dir}" \
  -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
  -DBUILD_SHARED_LIBS=OFF \
  -DINSTALL_DEPS_LIBS=ON \
  -G "Ninja" \
  ..
cmake --build . --parallel "${parallelism}" --target install
cd "${third_party_root}"

# hiredis
mkdir -p build_deps/hiredis && cd hiredis
mkdir -p "${build_folder_name}" && cd "${build_folder_name}"
cmake \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_CXX_STANDARD="${cc_standard}" \
  -DCMAKE_INSTALL_PREFIX="${third_party_root}/build_deps/hiredis" \
  -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
  -DBUILD_SHARED_LIBS=OFF \
  -G "Ninja" \
  ..
cmake --build . --parallel "${parallelism}" --target install
cd "${third_party_root}"