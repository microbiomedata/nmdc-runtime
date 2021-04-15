from dagster import ModeDefinition, pipeline, resource, Field, String
from terminusdb_client import WOQLClient, WOQLQuery as WQ

from nmdc_runtime.solids.hello import hello, list_databases

# Mode definitions allow you to configure the behavior of your pipelines and solids at execution
# time. For hints on creating modes in Dagster, see our documentation overview on Modes and
# Resources: https://docs.dagster.io/overview/modes-resources-presets/modes-resources


class TerminusDB:
    def __init__(self, server_url, user, key, account, db):
        self.client = WOQLClient(server_url=server_url)
        # FIXME operation timed out - failed to establish a new connection
        self.client.connect(account=account, db=db, user=user, key=key)


@resource(
    config_schema={
        "server_url": Field(String),
        "user": Field(String),
        "key": Field(String),
        "account": Field(String),
        "db": Field(String),
    }
)
def terminus_resource(context):
    return TerminusDB(
        server_url=context.resource_config["server_url"],
        user=context.resource_config["user"],
        key=context.resource_config["key"],
        account=context.resource_config["account"],
        db=context.resource_config["db"],
    )


MODE_DEV = ModeDefinition(name="dev", resource_defs={"terminus": terminus_resource})
MODE_TEST = ModeDefinition(name="test", resource_defs={"terminus": terminus_resource})


@pipeline(mode_defs=[MODE_DEV, MODE_TEST])
def my_pipeline():
    """
    A pipeline definition. This example pipeline has a single solid.

    For more hints on writing Dagster pipelines, see our documentation overview on Pipelines:
    https://docs.dagster.io/overview/solids-pipelines/pipelines
    """
    hello()


@pipeline(mode_defs=[MODE_DEV, MODE_TEST])
def another_pipeline():
    """
    A pipeline definition. This example pipeline has a single solid.

    For more hints on writing Dagster pipelines, see our documentation overview on Pipelines:
    https://docs.dagster.io/overview/solids-pipelines/pipelines
    """
    list_databases()
