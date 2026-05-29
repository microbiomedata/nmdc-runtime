from pathlib import Path
import shutil
import subprocess
from typing import Callable

from rich.console import Console
from rich.progress import Progress

from src.lib.display import LiveLogManager, make_live_display, make_progress_indicator_for_unbounded_task

console = Console()


def dump_subprocess_failure_and_raise_runtime_error(
    completed_process: subprocess.CompletedProcess[str],
    task_description: str,
    include_full_output_in_error: bool = True,
) -> None:
    """
    Dump information about the process that failed, and raise a `RuntimeError`.

    If `include_full_output_in_error` is `True`, this function will include all of the process's
    output among the information it dumps.
    """

    # Print metadata about the subprocess that failed.
    console.print(f"{task_description} failed.", markup=False, style="bold red")
    console.print(f"Command: {completed_process.args}", markup=False, style="red")
    console.print(f"Exit code: {completed_process.returncode}", markup=False, style="red")

    # If requested, print the STDOUT and STDERR streams of the subprocess. Note that, in this CLI
    # app, the `stdout` attribute of the `CompletedProcess` instance returned by `run_subprocess`
    # contains the combination of STDOUT and STDERR. We print `stderr` anyway, here, so this
    # function remains "general purpose."
    if include_full_output_in_error:
        console.print("Output:\n\n", markup=False, style="bold yellow")
        console.print(completed_process.stdout, markup=False, style="yellow")
        console.print(completed_process.stderr, markup=False, style="red")

    raise RuntimeError(f"{task_description} failed with exit code {completed_process.returncode}.")


def run_subprocess_with_live_display(
    command_parts: list[str],
    task_description: str,
    progress: Progress | None = None,
    on_error: Callable[[subprocess.CompletedProcess[str], str, bool], None] | None = None,
) -> subprocess.CompletedProcess[str]:
    """
    Run a subprocess with a live display of its progress and output, and return the completed process.

    When attached to a terminal/TTY, this function shows a transient Rich live display containing a
    progress indicator (if provided) and a limited-height panel containing the most recent lines of
    output from the subprocess.

    When not attached to a terminal/TTY, this function simply prints the lines of output from the
    subprocess as they come in.

    If the subprocess fails and an `on_error` callback is provided, this function will call it with
    the failed `CompletedProcess`, the `task_description`, and a boolean indicating whether the
    subprocess output was already emitted in persistent plain-text form.
    """

    if console.is_terminal:
        include_full_output_in_error = True  # necessary, since our panel only shows recent lines
        if progress is None:
            progress = make_progress_indicator_for_unbounded_task(auto_refresh=False)
            progress.add_task(description=task_description, total=None)
        with make_live_display(
            renderable=LiveLogManager.make_group_having_progress_and_log(progress, []),
            console=console,
        ) as live_display:
            live_log_manager = LiveLogManager(progress=progress, live_display=live_display, max_num_lines=5)
            completed_process = run_subprocess(
                command_parts,
                on_line_received=live_log_manager.add_line_and_update_live_display,
            )
    else:
        include_full_output_in_error = False  # unnecessary, since we stream it to console
        if task_description != "":
            console.print(task_description, markup=False)

        def stream_output_line_to_console(output_line: str) -> None:
            """Helper function that prints the line to the console."""
            console.print(output_line, markup=False, end="")

        completed_process = run_subprocess(
            command_parts,
            on_line_received=stream_output_line_to_console,
        )

    # If the subprocess failed and an `on_error` callback was provided, call it now.
    if completed_process.returncode != 0:
        if isinstance(on_error, Callable):
            on_error(completed_process, task_description, include_full_output_in_error)

    return completed_process


def run_subprocess(
    command_parts: list[str],
    on_line_received: Callable[[str], None] | None = None,
) -> subprocess.CompletedProcess[str]:
    r"""
    Run the specified command as a subprocess, capturing the STDOUT and STDERR output as a _single_
    string and including it in the `stdout` attribute of the returned `subprocess.CompletedProcess`.

    If an `on_line_received` callback is provided, then, while the subprocess runs, this function
    will call it whenever a new line of merged output becomes available, enabling upstream code to
    react to subprocess output in real time. Regardless of whether a handler is provided, the
    `stdout` attribute of the returned `CompletedProcess` will contain the full merged output
    (and `stderr` will contain `None`).

    The `returncode` attribute of the returned `CompletedProcess` instance will contain the exit
    status of the subprocess.

    Demo of capturing output and a zero exit code:
    >>> completed_process = run_subprocess(["echo", "hello world"])
    >>> completed_process.returncode
    0
    >>> completed_process.stdout.strip()
    'hello world'
    >>> completed_process.stderr is None
    True

    Contrived demo of capturing merged output and a non-zero exit code:
    >>> completed_process = run_subprocess([
    ...     "python", "-c",
    ...     "import sys; sys.stderr.write('hello error'); sys.exit(1)"
    ... ])
    >>> completed_process.returncode
    1
    >>> completed_process.stdout.strip()
    'hello error'
    >>> completed_process.stderr is None
    True

    Examining the returned `CompletedProcess` instance when an output line handler was provided:
    >>> completed_process = run_subprocess(
    ...     [ "echo", "First line\nSecond line" ],
    ...     on_line_received=lambda line: print("! " + line, end=""),
    ... )
    ! First line
    ! Second line
    >>> completed_process.returncode
    0
    >>> completed_process.stdout.strip()
    'First line\nSecond line'
    >>> completed_process.stderr is None
    True
    """

    output_lines: list[str] = []

    # Run the subprocess. Use `Popen` so we can capture the merged output in real time (and forward
    # it to the `on_line_received`, if provided).
    # Docs: https://docs.python.org/3/library/subprocess.html#popen-constructor
    popen = subprocess.Popen(
        command_parts,
        # Merge the STDERR stream into the STDOUT stream.
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    if popen.stdout is None:
        raise RuntimeError("Failed to access STDOUT and/or STDERR stream of subprocess.")

    # Whenever we receive an additional line from the merged stream, store the line (so we can
    # eventually return all lines) and propagate it to the `on_line_received`, if provided.
    for output_line in popen.stdout:
        output_lines.append(output_line)
        if isinstance(on_line_received, Callable):
            on_line_received(output_line)

    # Now that we've finished receiving output from the subprocess, wait for it to terminate,
    # and return the completed process (setting its `stdout` attribute to the full output).
    return_code: int = popen.wait(timeout=60)
    all_output = "".join(output_lines)
    return subprocess.CompletedProcess(
        args=command_parts,
        returncode=return_code,
        stdout=all_output,
        stderr=None,
    )


def ensure_pip_is_available(path_to_python_executable: str) -> None:
    """
    Checks whether pip is available in the environment associated with the specified Python binary,
    installing it there if it isn't available yet.
    """

    # Run a basic `pip` command (e.g. check its version) in order to check whether it's available.
    version_check_result = run_subprocess_with_live_display(
        [path_to_python_executable, "-m", "pip", "--version"],
        task_description="Checking pip availability",
    )

    # If the command succeeded, then pip is available and we're done.
    if version_check_result.returncode == 0:
        return None

    # Otherwise, check whether the reason for failure is anything _other_ than `pip` not being installed.
    elif "No module named pip" not in version_check_result.stdout:
        console.print(f"Combined STDOUT and STDERR:\n\n{version_check_result.stdout}", markup=False, style="red")
        raise RuntimeError("Failed to check pip version.")

    # If we reach this point, it means `pip` is not installed. So, we'll install it via `ensurepip`,
    # which is in the Python stdlib. Docs: https://docs.python.org/3.10/library/ensurepip.html
    bootstrap_result = run_subprocess_with_live_display(
        [path_to_python_executable, "-m", "ensurepip", "--upgrade"],
        task_description="Installing pip",
    )

    # If the `ensurepip` command failed, dump the failure info to the console and raise an error.
    if bootstrap_result.returncode != 0:
        console.print(f"Combined STDOUT and STDERR:\n\n{bootstrap_result.stdout}", markup=False, style="red")
        raise RuntimeError("Failed to install pip.")


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


def copy_contents_of_directory(path_to_source_dir: Path, path_to_destination_dir: Path) -> None:
    """
    Copies everything (i.e. files and folders) residing in the specified source directory into the
    specified destination directory; creating the latter directory if it doesn't already exist.

    >>> from tempfile import TemporaryDirectory
    >>> with TemporaryDirectory() as temp_dir_name:
    ...     temp_dir_path = Path(temp_dir_name)
    ...     source_dir_path = temp_dir_path / "source"
    ...     destination_dir_path = temp_dir_path / "destination"
    ...     source_dir_path.mkdir()
    ...     _ = (source_dir_path / "hello.txt").write_text("world")
    ...     copy_contents_of_directory(source_dir_path, destination_dir_path)
    ...     (destination_dir_path / "hello.txt").read_text()
    'world'
    >>> with TemporaryDirectory() as temp_dir_name:
    ...     temp_dir_path = Path(temp_dir_name)
    ...     copy_contents_of_directory(temp_dir_path, temp_dir_path)
    Traceback (most recent call last):
    ...
    ValueError: Paths to source and destination directories must be distinct.
    """

    if path_to_source_dir == path_to_destination_dir:
        raise ValueError("Paths to source and destination directories must be distinct.")

    # Ensure the destination directory exists.
    path_to_destination_dir.mkdir(parents=True, exist_ok=True)

    # Copy the contents of the source directory into the destination directory.
    _ = shutil.copytree(
        src=path_to_source_dir,
        dst=path_to_destination_dir,
        dirs_exist_ok=True,
        symlinks=True,
    )
