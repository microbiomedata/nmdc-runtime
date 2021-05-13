from dagster import ModeDefinition, pipeline, PresetDefinition

from nmdc_runtime.solids.jgi import build_merged_db, run_etl

from nmdc_runtime.pipelines.core import (
    mode_dev,
    mode_prod,
    preset_prod_env,
    preset_dev_env,
)


@pipeline(
    mode_defs=[mode_dev, mode_prod], preset_defs=[preset_dev_env, preset_prod_env]
)
def gold_etl():
    run_etl(build_merged_db())
