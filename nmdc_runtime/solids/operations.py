import json
from datetime import datetime, timezone

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


@solid
def filter_ops_undone_expired_object_puts() -> str:
    # TODO add schedule to periodically run this as part of a cleaning pipeline.
    return json.dumps(
        {
            "done": False,
            "metadata.model": dotted_path_for(ObjectPutMetadata),
            "expire_time": {"$lt": datetime.now(timezone.utc)},
        }
    )


@solid(required_resource_keys={"runtime_api_site_client"})
def list_operations(context, filter_: str) -> list:
    client = context.resources.runtime_api_site_client
    ops = [op.dict() for op in client.list_operations({"filter": filter_})]
    context.log.info(str(len(ops)))
    return ops


@solid(required_resource_keys={"mongo"})
def delete_operations(context, op_docs: list):
    mdb = context.resources.mongo.db
    rv = mdb.operations.delete_many({"id": {"$in": [doc["id"] for doc in op_docs]}})
    context.log.info(f"Deleted {rv.deleted_count} of {len(op_docs)}")
    if rv.deleted_count != len(op_docs):
        context.log.error("Didn't delete all.")
