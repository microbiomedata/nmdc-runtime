import json

from dagster import RunRequest, sensor

from nmdc_runtime.api.core.util import dotted_path_for
from nmdc_runtime.api.models.operation import ObjectPutMetadata
from nmdc_runtime.pipelines.core import (
    preset_normal_env,
    run_config_frozen__preset_normal_env,
)
from nmdc_runtime.resources.core import get_runtime_api_site_client
from nmdc_runtime.resources.mongo import get_mongo


@sensor(pipeline_name="gold_translation_curation", mode="normal")
def new_gold_translation(_context):
    client = get_runtime_api_site_client(
        run_config=run_config_frozen__preset_normal_env
    )
    mongo = get_mongo(run_config=run_config_frozen__preset_normal_env)
    gold_etl_latest = mongo.db.objects.find_one(
        {"name": "nmdc_database.json.zip"}, sort=[("created_time", -1)]
    )
    # TODO ensure job with id of latest object. if existing job
    #   with older object, cancel that job.
    should_run = len(ops) > 0
    if should_run:
        run_key = ",".join(sorted([op["id"] for op in ops]))
        yield RunRequest(run_key=run_key, run_config=preset_normal_env.run_config)
