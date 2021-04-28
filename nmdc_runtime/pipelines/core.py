from dagster import ModeDefinition, pipeline, PresetDefinition

from nmdc_runtime.resources.mongo import mongo_resource
from nmdc_runtime.solids.core import mongo_stats, update_schema, hello
from nmdc_runtime.resources.core import terminus_resource

# Mode definitions allow you to configure the behavior of your pipelines and solids at execution
# time. For hints on creating modes in Dagster, see our documentation overview on Modes and
# Resources: https://docs.dagster.io/overview/modes-resources-presets/modes-resources


mode_dev = ModeDefinition(
    name="dev",
    resource_defs={"terminus": terminus_resource, "mongo": mongo_resource},
)
mode_prod = ModeDefinition(
    name="prod",
    resource_defs={"terminus": terminus_resource, "mongo": mongo_resource},
)
mode_test = ModeDefinition(name="test")

# preset_dev_yaml = PresetDefinition.from_files(
#     "dev_via_yaml",
#     config_files=[
#         file_relative_path(__file__, "presets_dev.yaml"),
#     ],
#     mode="dev",
# )

preset_dev_env = PresetDefinition(
    "dev_via_env",
    run_config={
        "resources": {
            "terminus": {
                "config": {
                    "server_url": {"env": "DAGSTER_DEV_TERMINUS_SERVER_URL"},
                    "key": {"env": "DAGSTER_DEV_TERMINUS_KEY"},
                    "user": {"env": "DAGSTER_DEV_TERMINUS_USER"},
                    "account": {"env": "DAGSTER_DEV_TERMINUS_ACCOUNT"},
                    "dbid": {"env": "DAGSTER_DEV_TERMINUS_DBID"},
                },
            },
            "mongo": {
                "config": {
                    "host": {"env": "DAGSTER_DEV_MONGO_HOST"},
                    "username": {"env": "DAGSTER_DEV_MONGO_USERNAME"},
                    "password": {"env": "DAGSTER_DEV_MONGO_PASSWORD"},
                    "dbname": {"env": "DAGSTER_DEV_MONGO_DBNAME"},
                },
            },
        },
    },
    mode="dev",
)

preset_prod_env = PresetDefinition(
    "prod_via_env",
    run_config={
        "resources": {
            "terminus": {
                "config": {
                    "server_url": {"env": "DAGSTER_PROD_TERMINUS_SERVER_URL"},
                    "key": {"env": "DAGSTER_PROD_TERMINUS_KEY"},
                    "user": {"env": "DAGSTER_PROD_TERMINUS_USER"},
                    "account": {"env": "DAGSTER_PROD_TERMINUS_ACCOUNT"},
                    "dbid": {"env": "DAGSTER_PROD_TERMINUS_DBID"},
                },
            },
            "mongo": {
                "config": {
                    "host": {"env": "DAGSTER_PROD_MONGO_HOST"},
                    "username": {"env": "DAGSTER_PROD_MONGO_USERNAME"},
                    "password": {"env": "DAGSTER_PROD_MONGO_PASSWORD"},
                    "dbname": {"env": "DAGSTER_PROD_MONGO_DBNAME"},
                },
            },
        },
    },
    mode="prod",
)


@pipeline(
    mode_defs=[mode_dev, mode_prod], preset_defs=[preset_dev_env, preset_prod_env]
)
def update_terminus():
    """
    A pipeline definition. This example pipeline has a single solid.

    For more hints on writing Dagster pipelines, see our documentation overview on Pipelines:
    https://docs.dagster.io/overview/solids-pipelines/pipelines
    """
    update_schema()


@pipeline(
    mode_defs=[mode_dev, mode_prod], preset_defs=[preset_dev_env, preset_prod_env]
)
def hello_mongo():
    mongo_stats()


@pipeline(mode_defs=[mode_test])
def hello_pipeline():
    hello()
