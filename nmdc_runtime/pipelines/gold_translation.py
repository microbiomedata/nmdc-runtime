"""
Translating an export of the JGI GOLD [1] database of SQL tables to the NMDC database JSON schema.

[1] Genomes OnLine Database (GOLD) <https://gold.jgi.doe.gov/>.
"""

from dagster import pipeline

from nmdc_runtime.solids.core import local_file_to_api_object
from nmdc_runtime.solids.gold_translation import build_merged_db, run_etl

from nmdc_runtime.pipelines.core import (
    mode_normal,
    preset_normal_env,
)


@pipeline(mode_defs=[mode_normal], preset_defs=[preset_normal_env])
def gold_translation():
    local_file_to_api_object(run_etl(build_merged_db()))
