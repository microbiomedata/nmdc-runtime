from dagster import solid

from nmdc_runtime.resources.core import RuntimeApiSiteClient


@solid(required_resource_keys={"runtime_api_site_client"})
def create_objects_from_done_object_puts(context, op_docs: list):
    client: RuntimeApiSiteClient = context.resources.runtime_api_site_client
    for doc in op_docs:
        pass
    context.log.info("OK")
    return "OK"
