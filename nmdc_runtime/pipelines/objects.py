import json

from dagster import pipeline

from nmdc_runtime.solids.operations import list_operations, filter_ops_done_object_puts

from nmdc_runtime.pipelines.core import (
    mode_normal,
    preset_normal_env,
)


@pipeline(mode_defs=[mode_normal], preset_defs=[preset_normal_env])
def create_object_from_site_object_put():
    # TODO need to stub input through config. can only pass solid output directly.
    list_operations(filter_ops_done_object_puts())
