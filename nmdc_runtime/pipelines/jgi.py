from dagster import ModeDefinition, pipeline, PresetDefinition

from nmdc_runtime.solids.jgi import get_json_db
from nmdc_runtime.solids.core import hello

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
    hello()
    get_json_db()
