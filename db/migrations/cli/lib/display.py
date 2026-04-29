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


def get_progress_columns_for_bounded_task() -> tuple[ProgressColumn, ...]:
    """
    Returns a tuple of `rich.progress.ProgressColumn` instances to be used for a bounded task.
    """

    return (
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        MofNCompleteColumn(),
        TimeRemainingColumn(),
    )


def make_progress_indicator_for_unbounded_task() -> Progress:
    """
    Returns a `rich.progress.Progress` instance configured with appropriate columns for an unbounded task
    (i.e. a task for which the developer does _not_ know the total number of steps in advance).
    """

    return Progress(
        *get_progress_columns_for_unbounded_task(),
        refresh_per_second=1,
        transient=True,
    )


def make_progress_indicator_for_bounded_task() -> Progress:
    """
    Returns a `rich.progress.Progress` instance configured with appropriate columns for a bounded task
    (i.e. a task for which the developer _does_ know the total number of steps in advance).
    """

    return Progress(
        *get_progress_columns_for_bounded_task(),
        refresh_per_second=1,
        transient=True,
    )
