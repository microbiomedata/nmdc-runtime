from dagster import RunRequest, sensor

from nmdc_runtime.pipelines.core import preset_normal_env


@sensor(pipeline_name="hello_mongo", mode="normal")
def my_sensor(_context):
    """
    A sensor definition. This example sensor always requests a pipeline run at each sensor tick.

    For more hints on running pipelines with sensors in Dagster, see our documentation overview on
    Sensors:
    https://docs.dagster.io/overview/schedules-sensors/sensors
    """
    should_run = True
    if should_run:
        yield RunRequest(run_key=None, run_config=preset_normal_env.run_config)
