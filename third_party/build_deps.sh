mkdir -p build_deps

third_party_root=$(pwd)
cc_standard=20
nproc=8

python_exec_prefix=$(python -c "import sys; print(sys.exec_prefix)")

abseil_install_dir="${third_party_root}/build_deps/abseil-cpp"
boost_install_dir="${third_party_root}/build_deps/boost"
cppitertools_install_dir="${third_party_root}/build_deps/cppitertools"
googletest_install_dir="${third_party_root}/build_deps/googletest"
poco_install_dir="${third_party_root}/build_deps/poco"
pybind11_install_dir="${third_party_root}/build_deps/pybind11"
pybind11_abseil_install_dir="${third_party_root}/build_deps/pybind11_abseil"

# Abseil
mkdir -p build_deps/abseil-cpp
cd abseil-cpp || exit 1
mkdir -p build
cd build || exit 1
cmake \
  -DABSL_BUILD_TESTING=ON \
  -DCMAKE_CXX_STANDARD="${cc_standard}" \
  -DCMAKE_INSTALL_PREFIX="${abseil_install_dir}" \
  -G "Ninja" \
  .. || exit 1
rm -rf "${abseil_install_dir}"
cmake --build . --parallel "${nproc}" --target install || exit 1
cd "${third_party_root}" || exit 1

# Google Test
mkdir -p build_deps/googletest
cd googletest || exit 1
mkdir -p build
cd build || exit 1
cmake \
  -DCMAKE_PREFIX_PATH="${abseil_install_dir}" \
  -DCMAKE_CXX_STANDARD="${cc_standard}" \
  -DCMAKE_INSTALL_PREFIX="${googletest_install_dir}" \
  -G "Ninja" \
  .. || exit 1
rm -rf "${googletest_install_dir}"
cmake --build . --parallel "${nproc}" --target install || exit 1
cd "${third_party_root}" || exit 1

# Boost
mkdir -p build_deps/boost
cd boost || exit 1
mkdir -p build
cd build || exit 1
cmake \
  -DCMAKE_CXX_STANDARD="${cc_standard}" \
  -DCMAKE_INSTALL_PREFIX="${boost_install_dir}" \
  -DBOOST_INCLUDE_LIBRARIES="fiber;intrusive" \
  -DBOOST_ENABLE_CMAKE=ON \
  -G "Ninja" \
  .. || exit 1
rm -rf "${boost_install_dir}"
cmake --build . --parallel "${nproc}" --target install || exit 1
cd "${third_party_root}" || exit 1

# Poco
mkdir -p build_deps/poco
cd poco || exit 1
mkdir -p build
cd build || exit 1
cmake \
  -DCMAKE_CXX_STANDARD="${cc_standard}" \
  -DCMAKE_INSTALL_PREFIX="${poco_install_dir}" \
  -G "Ninja" \
  .. || exit 1
rm -rf "${poco_install_dir}"
cmake --build . --parallel "${nproc}" --target install || exit 1
cd "${third_party_root}" || exit 1

# Pybind11
mkdir -p build_deps/pybind11
cd pybind11 || exit 1
mkdir -p build
cd build || exit 1
cmake \
  -DCMAKE_CXX_STANDARD="${cc_standard}" \
  -DCMAKE_INSTALL_PREFIX="${pybind11_install_dir}" \
  -DPython_ROOT_DIR="${python_exec_prefix}" \
  -DPython3_ROOT_DIR="${python_exec_prefix}" \
  -G "Ninja" \
  .. || exit 1
rm -rf "${pybind11_install_dir}"
cmake --build . --parallel "${nproc}" --target install || exit 1
cd "${third_party_root}" || exit 1

# cppitertools
mkdir -p build_deps/cppitertools
cd cppitertools || exit 1
mkdir -p __build
cd __build || exit 1
cmake \
  -DCMAKE_CXX_STANDARD="${cc_standard}" \
  -DCMAKE_INSTALL_PREFIX="${cppitertools_install_dir}" \
  -G "Ninja" \
  .. || exit 1
rm -rf "${cppitertools_install_dir}"
cmake --build . --parallel "${nproc}" --target install || exit 1
cd "${third_party_root}" || exit 1

