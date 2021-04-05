from dagster import repository

from nmdc_runtime.pipelines.my_pipeline import my_pipeline
from nmdc_runtime.schedules.my_hourly_schedule import my_hourly_schedule
from nmdc_runtime.sensors.my_sensor import my_sensor


@repository
def nmdc_runtime():
    """
    The repository definition for this nmdc_runtime Dagster repository.

    For hints on building your Dagster repository, see our documentation overview on Repositories:
    https://docs.dagster.io/overview/repositories-workspaces/repositories
    """
    pipelines = [my_pipeline]
    schedules = [my_hourly_schedule]
    sensors = [my_sensor]

    return pipelines + schedules + sensors
