from dagster import ModeDefinition, pipeline, PresetDefinition, file_relative_path

from nmdc_runtime.solids.core import list_databases, update_schema, hello
from nmdc_runtime.resources.core import terminus_resource

# Mode definitions allow you to configure the behavior of your pipelines and solids at execution
# time. For hints on creating modes in Dagster, see our documentation overview on Modes and
# Resources: https://docs.dagster.io/overview/modes-resources-presets/modes-resources


mode_dev = ModeDefinition(name="dev", resource_defs={"terminus": terminus_resource})
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
                }
            }
        }
    },
    mode="dev",
)


@pipeline(mode_defs=[mode_dev], preset_defs=[preset_dev_env])
def mongo_to_terminus():
    """
    A pipeline definition. This example pipeline has a single solid.

    For more hints on writing Dagster pipelines, see our documentation overview on Pipelines:
    https://docs.dagster.io/overview/solids-pipelines/pipelines
    """
    update_schema()


@pipeline(mode_defs=[mode_test])
def hello_pipeline():
    hello()
