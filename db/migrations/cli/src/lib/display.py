from collections import deque
from collections.abc import Iterable

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


class LiveLogManager:
    """
    Helper class that can be used to manage a queue of log lines and update a live display.
    """

    def __init__(
        self,
        progress: Progress,
        live_display: Live,
        max_num_lines: int | None = None,
    ) -> None:
        """Initializes the log line manager."""

        self._progress = progress
        self._live_display = live_display

        # Create a queue, having the specified length (`None` means no maximum), to hold the lines.
        #
        # Note: Although we only need a single-ended queue here, we used a "double-ended queue"
        #       because (a) we didn't see a single-ended queue in the Python stdlib, and (b) it
        #       can be instantiated so concisely like this. We only pull lines from a single end.
        #
        self._lines: deque[str] = deque(maxlen=max_num_lines)

    def add_line_and_update_live_display(self, line: str) -> None:
        """
        Adds a line to the queue, then updates the specified live display with a renderable built
        from the current lines in the queue and the specified progress indicator.
        """

        self._lines.append(line)
        self._live_display.update(
            self.make_group_having_progress_and_log(
                progress=self._progress,
                log_lines=self._lines,
                log_height_in_lines=self._lines.maxlen,
            )
        )

    @staticmethod
    def make_group_having_progress_and_log(
        progress: Progress,
        log_lines: Iterable[str],
        log_height_in_lines: int | None = None,
    ) -> Group:
        """
        Returns a `rich.console.Group` instance containing the specified `rich.progress.Progress` instance
        and a `rich.panel.Panel` instance displaying the specified log lines.

        This helper function was designed to be used to generate a `Renderable` that is passed to
        `live.update()`.
        """

        # If the caller didn't specify a log height, then we'll fall back to showing 1 line.
        if log_height_in_lines is None:
            log_height_in_lines = 1

        panel_border_height = 2  # 1 for top border, 1 for bottom border
        panel_height = log_height_in_lines + panel_border_height

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
