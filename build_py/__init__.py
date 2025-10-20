"""
A fully custom PEP 517 build backend that manually builds a .whl file
without setuptools or other build backends.
"""

import multiprocessing
import os
import platform
import sys
import json
import shutil
import subprocess
import tempfile
import zipfile
from pathlib import Path
from setuptools import build_meta

NAME = "actionengine"
VERSION = "0.1.2"
REPO_ROOT = Path(__file__).parent.parent.resolve()


def get_arch_tag():
    """Return the architecture tag for this build."""
    machine = platform.machine().lower()
    if "arm" in machine or "aarch64" in machine:
        return "arm64"

    return "x86_64"


def get_platform_tag():
    """Return the platform tag for this build."""
    arch = get_arch_tag()

    if sys.platform.startswith("linux"):
        return f"manylinux_2_17_{arch}"
    elif sys.platform == "darwin":
        return f"macosx_15_0_{arch}"
    elif sys.platform == "win32":
        return "win_amd64" if arch == "x86_64" else "win_arm64"
    else:
        raise RuntimeError(f"Unsupported platform: {sys.platform}")


def get_tag():
    """Return the wheel tag for this build."""
    py_version = f"py{sys.version_info.major}{sys.version_info.minor}"
    abi_tag = "abi3"
    return f"{py_version}-{abi_tag}-{get_platform_tag()}"


def build_wheel(wheel_directory, config_settings=None, metadata_directory=None):
    print(">>> Custom build_wheel(): building a pure wheel manually")

    shutil.rmtree(REPO_ROOT / "build", ignore_errors=True)

    print("Cleaning previous builds...")
    for so_file in (REPO_ROOT / "py" / "actionengine").glob("*.so"):
        print(f"Removing {so_file}")
        os.remove(so_file)

    stub_path = REPO_ROOT / "py" / "actionengine" / "_C"
    if stub_path.exists():
        shutil.rmtree(stub_path)

    os.chdir(REPO_ROOT)
    # Build the C++ extensions:
    if (
        subprocess.Popen(
            [str(REPO_ROOT / "scripts" / "configure.sh")],
            env=os.environ,
        )
    ).wait() != 0:
        raise RuntimeError("Build failed during configure step.")

    if (
        subprocess.Popen(
            [
                str(REPO_ROOT / "scripts" / "build_python.sh"),
                "--only-rebuild-pybind11",
            ],
            env=os.environ,
        ).wait()
        != 0
    ):
        raise RuntimeError("Build failed during cmake build step.")
    #
    # if (
    #     subprocess.Popen(
    #         [str(REPO_ROOT / "scripts" / "generate_stubs.sh")],
    #         env=os.environ,
    #     ).wait()
    #     != 0
    # ):
    #     raise RuntimeError("Build failed during stub generation step.")

    # Create a temporary directory for our wheel contents
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir = Path(temp_dir)

        # Copy package files into the temp directory
        pkg_target = temp_dir / NAME
        shutil.copytree((REPO_ROOT / "py" / "actionengine"), pkg_target)

        # Generate METADATA
        dist_info = temp_dir / f"{NAME}-{VERSION}.dist-info"
        dist_info.mkdir()
        (dist_info / "METADATA").write_text(
            f"""Metadata-Version: 2.1
Name: {NAME}
Version: {VERSION}
"""
        )

        # Generate WHEEL file
        (dist_info / "WHEEL").write_text(
            f"""Wheel-Version: 1.0
Generator: actionengine
Root-Is-Purelib: false
Tag: {get_tag()}
"""
        )

        # Write RECORD file (will list all files)
        record_lines = []
        for file_path in temp_dir.rglob("*"):
            if file_path.is_file():
                rel = file_path.relative_to(temp_dir)
                record_lines.append(f"{rel},,\n")

        (dist_info / "RECORD").write_text("".join(record_lines))

        # Build wheel filename
        wheel_name = f"{NAME}-{VERSION}-{get_tag()}.whl"
        wheel_path = Path(wheel_directory) / wheel_name

        # Zip it up
        with zipfile.ZipFile(wheel_path, "w", zipfile.ZIP_DEFLATED) as zf:
            for file_path in temp_dir.rglob("*"):
                if file_path.is_file():
                    zf.write(file_path, file_path.relative_to(temp_dir))

        print(f"✅ Built wheel: {wheel_path}")
        return wheel_name


def get_requires_for_build_wheel(config_settings=None):
    return build_meta.get_requires_for_build_wheel(config_settings)


def build_sdist(sdist_directory, config_settings=None):
    raise RuntimeError("SDist build is not supported in this custom backend.")
    print(">>> Custom build_sdist(): creating source tar.gz manually")
    import tarfile

    sdist_path = Path(sdist_directory) / f"{NAME}-{VERSION}.tar.gz"
    with tarfile.open(sdist_path, "w:gz") as tar:
        tar.add("src", arcname=f"{NAME}-{VERSION}/src")
        tar.add("pyproject.toml", arcname=f"{NAME}-{VERSION}/pyproject.toml")
        tar.add("build_backend", arcname=f"{NAME}-{VERSION}/build_backend")
    print(f"✅ Built source dist: {sdist_path}")
    return str(sdist_path)
