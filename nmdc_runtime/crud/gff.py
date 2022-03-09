from typing import List
from uuid import UUID

from nmdc_runtime.worker.gff_to_json import (
    convert_gffs_to_json,
    get_gff_task_results,
)


def gff_to_json(paths: List[str]) -> UUID:
    return convert_gffs_to_json(paths)


def add_gffs(group_id: UUID) -> None:
    get_gff_task_results(group_id)
    # Send to db
