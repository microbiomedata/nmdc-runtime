import json
from datetime import datetime, timezone

from bson import json_util
from dagster import solid, Failure

from nmdc_runtime.api.core.util import dotted_path_for
from nmdc_runtime.api.models.operation import ObjectPutMetadata, Operation


@solid
def filter_ops_done_object_puts() -> str:
    return json_util.dumps(
        {
            "done": True,
            "metadata.model": dotted_path_for(ObjectPutMetadata),
        }
    )


@solid
def filter_ops_undone_expired() -> str:
    return json_util.dumps(
        {
            "done": {"$ne": True},
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


@solid(required_resource_keys={"mongo"})
def get_operation(context):
    mdb = context.resources.mongo.db
    id_op = context.solid_config["operation_id"]
    doc = mdb.operations.find_one({"id": id_op})
    if doc is None:
        raise Failure(description=f"operation {id_op} not found")
    return Operation(**doc)
