from dagster import solid


@solid(required_resource_keys={"runtime_api_site_client"})
def list_operations(context, filter_: str) -> list:
    # TODO this is under schedule, so use conditional branching
    #     (https://docs.dagster.io/concepts/solids-pipelines/pipelines#conditional-branching)
    #     to bail early, i.e. ensure "guard" solid at head of pipeline.
    #     I think this solid could work well. Need to refactor it to yield multiple outputs.
    client = context.resources.runtime_api_site_client
    ops = [op.dict() for op in client.list_operations({"filter": filter_})]
    context.log.info(len(ops))
    return ops
