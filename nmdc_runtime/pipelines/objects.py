import json

from dagster import pipeline

from nmdc_runtime.api.core.util import dotted_path_for
from nmdc_runtime.api.models.operation import ObjectPutMetadata
from nmdc_runtime.solids.operations import list_operations

from nmdc_runtime.pipelines.core import (
    mode_normal,
    preset_normal_env,
)


@pipeline(mode_defs=[mode_normal], preset_defs=[preset_normal_env])
def create_object_from_site_object_put():
    # TODO this is under schedule, so use conditional branching
    #     (https://docs.dagster.io/concepts/solids-pipelines/pipelines#conditional-branching)
    #     to bail early, i.e. ensure "guard" solid at head of pipeline.
    #     I think list_operations could work well. Need to refactor it to yield multiple outputs.
    ops = list_operations(
        filter_=json.dumps(
            {
                "done": True,
                "metadata.model": dotted_path_for(ObjectPutMetadata),
            }
        )
    )
