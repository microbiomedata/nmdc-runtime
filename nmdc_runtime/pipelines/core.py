from dagster import ModeDefinition, pipeline, PresetDefinition

from nmdc_runtime.resources.core import runtime_api_site_client_resource
from nmdc_runtime.resources.mongo import mongo_resource
from nmdc_runtime.resources.terminus import terminus_resource
from nmdc_runtime.solids.core import mongo_stats, update_schema, hello


# Mode definitions allow you to configure the behavior of your pipelines and solids at execution
# time. For hints on creating modes in Dagster, see our documentation overview on Modes and
# Resources: https://docs.dagster.io/overview/modes-resources-presets/modes-resources
from nmdc_runtime.solids.operations import (
    filter_ops_undone_expired,
    delete_operations,
    list_operations,
)
from nmdc_runtime.util import frozendict_recursive

mode_normal = ModeDefinition(
    name="normal",
    resource_defs={
        "runtime_api_site_client": runtime_api_site_client_resource,
        "terminus": terminus_resource,
        "mongo": mongo_resource,
    },
)
mode_test = ModeDefinition(name="test")

run_config__preset_normal_env = {
    "resources": {
        "runtime_api_site_client": {
            "config": {
                "base_url": {"env": "API_HOST"},
                "site_id": {"env": "API_SITE_ID"},
                "client_id": {"env": "API_SITE_CLIENT_ID"},
                "client_secret": {"env": "API_SITE_CLIENT_SECRET"},
            },
        },
        "terminus": {
            "config": {
                "server_url": {"env": "TERMINUS_SERVER_URL"},
                "key": {"env": "TERMINUS_KEY"},
                "user": {"env": "TERMINUS_USER"},
                "account": {"env": "TERMINUS_ACCOUNT"},
                "dbid": {"env": "TERMINUS_DBID"},
            },
        },
        "mongo": {
            "config": {
                "host": {"env": "MONGO_HOST"},
                "username": {"env": "MONGO_USERNAME"},
                "password": {"env": "MONGO_PASSWORD"},
                "dbname": {"env": "MONGO_DBNAME"},
            },
        },
    },
}

preset_normal_env = PresetDefinition(
    "normal_via_env",
    run_config=run_config__preset_normal_env.copy(),
    mode="normal",
)

run_config_frozen__preset_normal_env = frozendict_recursive(
    run_config__preset_normal_env
)


@pipeline(mode_defs=[mode_test])
def hello_pipeline():
    hello()


@pipeline(mode_defs=[mode_normal], preset_defs=[preset_normal_env])
def hello_mongo():
    mongo_stats()


@pipeline(mode_defs=[mode_normal], preset_defs=[preset_normal_env])
def update_terminus():
    """
    A pipeline definition. This example pipeline has a single solid.

    For more hints on writing Dagster pipelines, see our documentation overview on Pipelines:
    https://docs.dagster.io/overview/solids-pipelines/pipelines
    """
    update_schema()


@pipeline(mode_defs=[mode_normal], preset_defs=[preset_normal_env])
def housekeeping():
    delete_operations(list_operations(filter_ops_undone_expired()))
