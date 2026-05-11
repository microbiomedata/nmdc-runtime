from collections import deque
from collections.abc import Iterable
from typing import Callable

from rich.console import Console, Group, RenderableType
from rich.live import Live
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    MofNCompleteColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
    ProgressColumn,
)


def get_progress_columns_for_unbounded_task() -> tuple[ProgressColumn, ...]:
    """
    Returns a tuple of `rich.progress.ProgressColumn` instances to be used for an unbounded task.
    """

    return (
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        TimeElapsedColumn(),
    )


def get_progress_columns_for_bounded_task(
    show_task_progress_percentage: bool = True,
) -> tuple[ProgressColumn, ...]:
    """
    Returns a tuple of `rich.progress.ProgressColumn` instances to be used for a bounded task.
    """

    return (
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn() if show_task_progress_percentage else TextColumn(""),
        TimeElapsedColumn(),
        MofNCompleteColumn(),
        TimeRemainingColumn(),
    )


def make_progress_indicator_for_unbounded_task(auto_refresh: bool = True) -> Progress:
    """
    Returns a `rich.progress.Progress` instance configured with appropriate columns for an unbounded task
    (i.e. a task for which the developer does _not_ know the total number of steps in advance).

    The `auto_refresh` parameter will be forwarded to the underlying `Progress` instance.
    """

    return Progress(
        *get_progress_columns_for_unbounded_task(),
        refresh_per_second=1,
        transient=True,
        auto_refresh=auto_refresh,
    )


def make_progress_indicator_for_bounded_task(
    auto_refresh: bool = True,
    show_task_progress_percentage: bool = True,
) -> Progress:
    """
    Returns a `rich.progress.Progress` instance configured with appropriate columns for a bounded task
    (i.e. a task for which the developer _does_ know the total number of steps in advance).

    The `auto_refresh` parameter will be forwarded to the underlying `Progress` instance.

    You can set `show_task_progress_percentage` to `False` in order to hide the percentage column.
    """

    return Progress(
        *get_progress_columns_for_bounded_task(show_task_progress_percentage=show_task_progress_percentage),
        refresh_per_second=1,
        transient=True,
        auto_refresh=auto_refresh,
    )


def make_live_display(renderable: RenderableType, console: Console) -> Live:
    """
    Returns a `rich.live.Live` instance configured for displaying a live feed of string content
    on the specified console.

    Docs: https://rich.readthedocs.io/en/latest/live.html
    """

    return Live(
        renderable=renderable,
        console=console,
        refresh_per_second=4,
        transient=True,
    )


def make_group_having_progress_and_log(
    progress: Progress,
    log_lines: Iterable[str],
    log_height_in_lines: int = 5,
) -> Group:
    """
    Returns a `rich.console.Group` instance containing the specified `rich.progress.Progress` instance
    and a `rich.panel.Panel` instance displaying the specified log lines.

    This helper function was designed to be used to generate a `Renderable` that is passed to
    `live.update()`.
    """

    panel_height = log_height_in_lines + 2  # +2 to account for the panel's top and bottom borders

    # Get the last `log_height_in_lines` lines from `log_lines` to display in the panel.
    log_lines_to_display = list(log_lines)[-log_height_in_lines:]

    return Group(
        progress,
        Panel(
            "".join(log_lines_to_display),  # each line already ends with a newline character
            height=panel_height,
            title="Log",
            border_style="dim",
        ),
    )


class LogLineManager:
    """
    Helper class to manage a queue of log lines and trigger a callback whenever a new line is added.
    """

    def __init__(self, max_num_lines: int | None = None) -> None:
        # Create a queue, having the specified length (`None` means no maximum), to hold the lines.
        #
        # Note: Although we only need a single-ended queue here, we used a "double-ended queue"
        #       because (a) we didn't see a single-ended queue in the Python stdlib, and (b) it
        #       can be instantiated so concisely like this. We only pull lines from a single end.
        #
        self._lines: deque[str] = deque(maxlen=max_num_lines)

    def add_line(self, line: str, callback_fn: Callable[[Iterable[str]], None]) -> None:
        """
        Adds a line to the queue, then calls the specified callback, passing it _all_ lines in the
        queue.

        This function was designed to be used to manage the log lines displayed in a Rich
        live display. The callback can be used to, for example, refresh the live display.
        """
        self._lines.append(line)
        callback_fn(self._lines)
