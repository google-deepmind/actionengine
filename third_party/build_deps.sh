#!/usr/bin/env bash

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
cppitertools_install_dir="${third_party_root}/build_deps/cppitertools"
googletest_install_dir="${third_party_root}/build_deps/googletest"
libdatachannel_install_dir="${third_party_root}/build_deps/libdatachannel"
pybind11_install_dir="${third_party_root}/build_deps/pybind11"

clear_build_env="0"
if [[ -n "${ACTIONENGINE_CLEAR_3P_BUILDS}" ]]; then
    clear_build_env="${ACTIONENGINE_CLEAR_3P_BUILDS}"
fi

# Google Test
mkdir -p build_deps/googletest
cd googletest
# if ACTIONENGINE_CLEAR_3P_BUILDS is set to 1, remove previous build folders
if [[ "${clear_build_env}" == "1" ]]; then
    rm -rf "${build_folder_name}"
    rm -rf "${googletest_install_dir}"
fi
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
# if ACTIONENGINE_CLEAR_3P_BUILDS is set to 1, remove previous build folders
if [[ "${clear_build_env}" == "1" ]]; then
    rm -rf "${build_folder_name}"
    rm -rf "${abseil_install_dir}"
fi
mkdir -p "${build_folder_name}"
cd "${build_folder_name}"
cmake \
  -DABSL_BUILD_TESTING=OFF \
  -DABSL_BUILD_TEST_HELPERS=ON \
  -DABSL_RUN_TESTS=OFF \
  -DINSTALL_GTEST=OFF \
  -DABSL_USE_EXTERNAL_GOOGLETEST=ON \
  -DGTest_DIR="${googletest_install_dir}/lib/cmake/gtest" \
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
# if ACTIONENGINE_CLEAR_3P_BUILDS is set to 1, remove previous build folders
if [[ "${clear_build_env}" == "1" ]]; then
    rm -rf "${build_folder_name}"
    rm -rf "${pybind11_install_dir}"
fi
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
# if ACTIONENGINE_CLEAR_3P_BUILDS is set to 1, remove previous build folders
if [[ "${clear_build_env}" == "1" ]]; then
    rm -rf "$__{build_folder_name}"
    rm -rf "${cppitertools_install_dir}"
fi
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
# if ACTIONENGINE_CLEAR_3P_BUILDS is set to 1, remove previous build folders
if [[ "${clear_build_env}" == "1" ]]; then
    rm -rf "${build_folder_name}"
    rm -rf "${third_party_root}/build_deps/libzmq"
fi
mkdir -p "${build_folder_name}" && cd "${build_folder_name}"
cmake \
  -DCMAKE_CXX_STANDARD="${cc_standard}" \
  -DCMAKE_INSTALL_PREFIX="${third_party_root}/build_deps/libzmq" \
  -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
  -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
  -DBUILD_TESTS=OFF \
  -DZMQ_BUILD_TESTS=OFF \
  -DBUILD_SHARED=OFF \
  -DBUILD_STATIC=ON \
  -G "Ninja" \
  ..
cmake --build . --parallel "${parallelism}" --target install
cd "${third_party_root}"

mkdir -p build_deps/cppzmq && cd cppzmq
# if ACTIONENGINE_CLEAR_3P_BUILDS is set to 1, remove previous build folders
if [[ "${clear_build_env}" == "1" ]]; then
    rm -rf "${build_folder_name}"
    rm -rf "${third_party_root}/build_deps/cppzmq"
fi
mkdir -p "${build_folder_name}" && cd "${build_folder_name}"
cmake \
  -DCMAKE_CXX_STANDARD="${cc_standard}" \
  -DCMAKE_PREFIX_PATH="${third_party_root}/build_deps/libzmq" \
  -DCMAKE_INSTALL_PREFIX="${third_party_root}/build_deps/cppzmq" \
  -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
  -DCPPZMQ_BUILD_TESTS=OFF \
  -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
  -G "Ninja" \
  ..
cmake --build . --parallel "${parallelism}" --target install
cd "${third_party_root}"

# libdatachannel
mkdir -p build_deps/libdatachannel && cd libdatachannel
# if ACTIONENGINE_CLEAR_3P_BUILDS is set to 1, remove previous build folders
if [[ "${clear_build_env}" == "1" ]]; then
    rm -rf "${build_folder_name}"
    rm -rf "${libdatachannel_install_dir}"
fi
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
# if ACTIONENGINE_CLEAR_3P_BUILDS is set to 1, remove previous build folders
if [[ "${clear_build_env}" == "1" ]]; then
    rm -rf "${build_folder_name}"
    rm -rf "${third_party_root}/build_deps/hiredis"
fi
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

# libuv
mkdir -p build_deps/libuv && cd libuv
# if ACTIONENGINE_CLEAR_3P_BUILDS is set to 1, remove previous build folders
if [[ "${clear_build_env}" == "1" ]]; then
    rm -rf "${build_folder_name}"
    rm -rf "${third_party_root}/build_deps/libuv"
fi
mkdir -p "${build_folder_name}" && cd "${build_folder_name}"
cmake \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_CXX_STANDARD="${cc_standard}" \
  -DCMAKE_INSTALL_PREFIX="${third_party_root}/build_deps/libuv" \
  -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
  -DLIBUV_BUILD_SHARED=OFF \
  -G "Ninja" \
  ..
cmake --build . --parallel "${parallelism}" --target install
cd "${third_party_root}"


# uvw
mkdir -p build_deps/uvw && cd uvw
# if ACTIONENGINE_CLEAR_3P_BUILDS is set to 1, remove previous build folders
if [[ "${clear_build_env}" == "1" ]]; then
    rm -rf "${build_folder_name}"
    rm -rf "${third_party_root}/build_deps/uvw"
fi
mkdir -p "${build_folder_name}" && cd "${build_folder_name}"
cmake \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_CXX_STANDARD="${cc_standard}" \
  -DCMAKE_INSTALL_PREFIX="${third_party_root}/build_deps/uvw" \
  -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
  -DUVW_BUILD_SHARED_LIB=OFF \
  -DUVW_BUILD_LIBS=ON \
  -G "Ninja" \
  ..
cmake --build . --parallel "${parallelism}" --target install
cd "${third_party_root}"

# protobuf
mkdir -p build_deps/protobuf && cd protobuf
# if ACTIONENGINE_CLEAR_3P_BUILDS is set to 1, remove previous build folders
if [[ "${clear_build_env}" == "1" ]]; then
    rm -rf "${build_folder_name}"
    rm -rf "${third_party_root}/build_deps/protobuf"
fi
mkdir -p "${build_folder_name}" && cd "${build_folder_name}"
cmake \
  -DBUILD_SHARED_LIBS=OFF \
  -Dprotobuf_BUILD_TESTS=OFF \
  -Dprotobuf_BUILD_PROTOC_BINARIES=ON \
  -Dabsl_DIR="${abseil_install_dir}/lib/cmake/absl" \
  -DGTest_DIR="${googletest_install_dir}/lib/cmake/gtest" \
  -DCMAKE_CXX_STANDARD="${cc_standard}" \
  -DCMAKE_INSTALL_PREFIX="${third_party_root}/build_deps/protobuf" \
  -DCMAKE_PREFIX_PATH="${third_party_root}/build_deps/protobuf:${third_party_root}/build_deps/googletest:${third_party_root}/build_deps/abseil-cpp" \
  -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
  -G "Ninja" \
  ..
cmake --build . --parallel "${parallelism}" --target install
cd "${third_party_root}"