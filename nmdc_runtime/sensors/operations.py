import json

from dagster import RunRequest, sensor

from nmdc_runtime.api.core.util import dotted_path_for
from nmdc_runtime.api.models.operation import ObjectPutMetadata
from nmdc_runtime.pipelines.core import preset_normal_env
from nmdc_runtime.resources.core import get_runtime_api_site_client


@sensor(pipeline_name="create_objects_from_site_object_puts", mode="normal")
def done_object_put_ops(_context):
    client = get_runtime_api_site_client(run_config=preset_normal_env.run_config)
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
        run_key = ",".join(sorted([op["id"] for op in ops]))
        yield RunRequest(run_key=run_key, run_config=preset_normal_env.run_config)
