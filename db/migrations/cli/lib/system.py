import subprocess

import typer


def run_subprocess(command_parts: list[str]) -> subprocess.CompletedProcess[str]:
    """Run a subprocess and capture its text output."""

    return subprocess.run(command_parts, capture_output=True, text=True)


def ensure_pip_is_available(python_executable: str) -> None:
    """Install `pip` into the specified Python environment if it is missing."""

    probe_result = run_subprocess([python_executable, "-m", "pip", "--version"])
    if probe_result.returncode == 0:
        return

    if "No module named pip" not in probe_result.stderr:
        raise typer.BadParameter(f"Failed to probe pip availability.\n\n{probe_result.stderr}")

    bootstrap_result = run_subprocess([python_executable, "-m", "ensurepip", "--upgrade"])
    if bootstrap_result.returncode != 0:
        raise typer.BadParameter(f"Failed to bootstrap pip.\n\n{bootstrap_result.stderr}")
