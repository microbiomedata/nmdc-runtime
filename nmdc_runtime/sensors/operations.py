import json

from dagster import build_init_resource_context, RunRequest, sensor
from toolz import get_in

from nmdc_runtime.api.core.util import dotted_path_for
from nmdc_runtime.api.models.operation import ObjectPutMetadata
from nmdc_runtime.pipelines.core import preset_normal_env
from nmdc_runtime.resources.core import runtime_api_site_client_resource


@sensor(pipeline_name="create_object_from_site_object_put", mode="normal")
def done_object_put_ops(_context):
    resource_context = build_init_resource_context(
        config=get_in(
            ["resources", "runtime_api_site_client", "config"],
            preset_normal_env.run_config,
        )
    )
    client = runtime_api_site_client_resource(resource_context)
    ops = [
        op.dict()
        for op in client.list_operations(
            {
                "filter": json.dumps(
                    {
                        "done": True,
                        "metadata.model": dotted_path_for(ObjectPutMetadata),
                    }
                )
            }
        )
    ]
    should_run = len(ops) > 0
    if should_run:
        yield RunRequest(run_key=None, run_config=preset_normal_env.run_config)
