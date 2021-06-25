from dagster import repository

from nmdc_runtime.pipelines.core import hello_mongo, update_terminus
from nmdc_runtime.pipelines.jgi import gold_etl
from nmdc_runtime.pipelines.objects import create_objects_from_site_object_puts
from nmdc_runtime.schedules.my_hourly_schedule import my_hourly_schedule
from nmdc_runtime.sensors.core import my_sensor
from nmdc_runtime.sensors.operations import done_object_put_ops


@repository
def nmdc_runtime():
    """
    The repository definition for this nmdc_runtime Dagster repository.

    For hints on building your Dagster repository, see our documentation overview on Repositories:
    https://docs.dagster.io/overview/repositories-workspaces/repositories
    """
    pipelines = [
        hello_mongo,
        update_terminus,
        create_objects_from_site_object_puts,
        gold_etl,
    ]
    schedules = [my_hourly_schedule]
    sensors = [my_sensor, done_object_put_ops]

    return pipelines + schedules + sensors
