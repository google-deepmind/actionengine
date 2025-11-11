import argparse
import os
import subprocess


def _parse_otool_line(line: str):
    """
    Parse a single line of otool -L output.

    Args:
        line (str): A line from the otool -L output.

    Returns:
        tuple: A tuple containing the dylib path and its version info.
    """
    parts = line.strip().split(" (")
    dylib_path = parts[0]
    version_info = parts[1][:-1] if len(parts) > 1 else ""
    return dylib_path, version_info


def patch_macos_dylib_path(
    lib_or_binary_path: str,
    dylib_filename: str,
    new_path: str,
    ignore_errors: bool = False,
):
    """
    Patch the RPATH of a macOS binary or library.

    Args:
        lib_or_binary_path (str): Path to the compiled file.
        dylib_filename (str): Filename of the dylib to patch.
        new_path (str): New path to set to load the dylib from.
    """

    # Check if the platform is macOS
    if os.uname().sysname != "Darwin":
        raise RuntimeError("This function is only supported on macOS.")

    # Use otool to find the current path of the dylib
    otool_cmd = ["otool", "-L", lib_or_binary_path]
    otool_output = subprocess.check_output(otool_cmd).decode("utf-8")
    lines = otool_output.strip().split("\n")[1:]  # Skip the first line
    dylib_path = None
    for line in lines:
        path, _ = _parse_otool_line(line)
        if os.path.basename(path) == dylib_filename:
            dylib_path = path
            break

    if dylib_path is None:
        if ignore_errors:
            print(
                f"Warning: Dylib {dylib_filename} not linked in {lib_or_binary_path}. "
                "Ignoring error as per the flag."
            )
            return
        raise RuntimeError(
            f"Dylib {dylib_filename} not found in {lib_or_binary_path}."
        )

    # Use install_name_tool to change the dylib path
    new_dylib_path = os.path.join(new_path, dylib_filename)
    install_name_tool_cmd = [
        "install_name_tool",
        "-change",
        dylib_path,
        new_dylib_path,
        lib_or_binary_path,
    ]
    subprocess.check_call(install_name_tool_cmd)
    print(
        f"Patched {lib_or_binary_path}: "
        f"{dylib_filename} from {dylib_path} to {new_dylib_path}"
    )


def main(args: argparse.Namespace):
    patch_macos_dylib_path(
        lib_or_binary_path=args.lib_or_binary_path,
        dylib_filename=args.dylib_filename,
        new_path=args.new_path,
        ignore_errors=args.ignore_errors,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Patch the RPATH of a macOS binary or library."
    )
    parser.add_argument(
        "lib_or_binary_path",
        type=str,
        help="Path to the compiled file (binary or library) to patch.",
    )
    parser.add_argument(
        "dylib_filename",
        type=str,
        help="Filename of the dylib to patch (e.g., libexample.dylib).",
    )
    parser.add_argument(
        "new_path",
        type=str,
        help="New path to set for loading the dylib.",
    )
    parser.add_argument(
        "--ignore-errors",
        action="store_true",
        help="Ignore errors during patching.",
    )
    main(parser.parse_args())
