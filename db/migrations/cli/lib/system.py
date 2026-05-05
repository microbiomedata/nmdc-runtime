from pathlib import Path
import shutil
import subprocess

import typer


def run_subprocess(command_parts: list[str]) -> subprocess.CompletedProcess[str]:
    """
    Run the specific command as a subprocess and capture its output as text.

    Callers of this function can check the exit status via the `returncode` attribute
    of the returned `CompletedProcess` object.

    Demo of capturing STDOUT and a zero exit code:
    >>> completed_process = run_subprocess([
    ...     "echo", "hello world"
    ... ])
    >>> completed_process.returncode
    0
    >>> completed_process.stdout.strip()
    'hello world'
    >>> completed_process.stderr.strip() == ''
    True

    Contrived demo of capturing STDERR and a non-zero exit code:
    >>> completed_process = run_subprocess([
    ...     "python", "-c",
    ...     "import sys; sys.stderr.write('hello error'); sys.exit(1)"
    ... ])
    >>> completed_process.returncode
    1
    >>> completed_process.stdout.strip() == ''
    True
    >>> completed_process.stderr.strip()
    'hello error'
    """

    return subprocess.run(command_parts, capture_output=True, text=True)


def ensure_pip_is_available(path_to_python_executable: str) -> None:
    """Install `pip` into the specified Python environment if it is missing."""

    probe_result = run_subprocess([path_to_python_executable, "-m", "pip", "--version"])
    if probe_result.returncode == 0:
        return

    if "No module named pip" not in probe_result.stderr:
        raise typer.BadParameter(f"Failed to probe pip availability.\n\n{probe_result.stderr}")

    bootstrap_result = run_subprocess([path_to_python_executable, "-m", "ensurepip", "--upgrade"])
    if bootstrap_result.returncode != 0:
        raise typer.BadParameter(f"Failed to bootstrap pip.\n\n{bootstrap_result.stderr}")


def is_directory_empty(path_to_dir: Path) -> bool:
    """Returns `True` if the specified directory is empty (i.e. contains no files or folders)."""

    return not any(path_to_dir.iterdir())


def delete_contents_of_directory(path_to_dir: Path) -> bool:
    """
    Deletes everything (i.e. files and folders) residing in the specified directory.
    Returns `True` if the directory ends up being empty.
    """

    for path in path_to_dir.iterdir():
        # Note: In Python, `path.is_dir()` returns True for symlinks to directories, since the
        #       symlink's target is a directory. Calling `shutil.rmtree(path)` would delete the
        #       contents of that directory, which I do _not_ want to do. Instead, I just want to
        #       delete the symlink, itself, from _this_ directory.
        if path.is_symlink():  # symlink to either a file or a directory
            path.unlink()
        elif path.is_dir():  # regular directory (not a symlink)
            shutil.rmtree(path)
        else:  # regular file (not a symlink)
            path.unlink()

    return is_directory_empty(path_to_dir=path_to_dir)
