from dagster import solid, Failure

from nmdc_runtime.resources.core import RuntimeApiSiteClient


@solid(required_resource_keys={"runtime_api_site_client"})
def create_objects_from_ops(context, op_docs: list):
    client: RuntimeApiSiteClient = context.resources.runtime_api_site_client
    responses = [client.create_object_from_op(doc) for doc in op_docs]
    if {r.status_code for r in responses} == {201}:
        context.log.info("All OK")
    else:
        raise Failure(f"Unexpected response(s): {[r.text for r in responses]}")
    return op_docs
