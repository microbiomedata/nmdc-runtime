from dagster import pipeline

from nmdc_runtime.solids.objects import create_objects_from_ops
from nmdc_runtime.solids.operations import (
    list_operations,
    filter_ops_done_object_puts,
    delete_operations,
)

from nmdc_runtime.pipelines.core import (
    mode_normal,
    preset_normal_env,
)


@pipeline(mode_defs=[mode_normal], preset_defs=[preset_normal_env])
def create_objects_from_site_object_puts():
    delete_operations(
        create_objects_from_ops(list_operations(filter_ops_done_object_puts()))
    )
