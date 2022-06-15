from typing import List, Any
from uuid import UUID

from celery import group
from celery.result import GroupResult

from nmdc_runtime.lib.gff_to_json import generate_counts
from nmdc_runtime.worker.task_manager import get_tm

tm = get_tm()


@tm.task(acks_late=True)
def gff_to_json_task(path: str) -> Any:
    result = generate_counts(
        path,
        path + ".md5",
        "mga0xhja88",
    )
    return result


def convert_gffs_to_json(paths: List[str]) -> UUID:
    """
    Takes a list of gff files and converts them into a json file.
    """
    job = group(gff_to_json_task.s(path) for path in paths)
    result = job.apply_async()
    return result.id


def get_gff_task_results(id: UUID) -> Any:
    results = GroupResult(id)
    print(results)
    return results.get()
