import json

from dagster import solid

from nmdc_runtime.api.core.util import dotted_path_for
from nmdc_runtime.api.models.operation import ObjectPutMetadata


@solid
def filter_ops_done_object_puts() -> str:
    return json.dumps(
        {
            "done": True,
            "metadata.model": dotted_path_for(ObjectPutMetadata),
        }
    )


@solid(required_resource_keys={"runtime_api_site_client"})
def list_operations(context, filter_: str) -> list:
    client = context.resources.runtime_api_site_client
    ops = [op.dict() for op in client.list_operations({"filter": filter_})]
    context.log.info(str(len(ops)))
    return ops
