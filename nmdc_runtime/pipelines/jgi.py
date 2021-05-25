from dagster import pipeline

from nmdc_runtime.solids.jgi import build_merged_db, run_etl

from nmdc_runtime.pipelines.core import (
    mode_normal,
    preset_normal_env,
)


@pipeline(mode_defs=[mode_normal], preset_defs=[preset_normal_env])
def gold_etl():
    run_etl(build_merged_db())
