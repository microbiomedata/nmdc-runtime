import json

from dagster import RunRequest, sensor, SkipReason, AssetKey, asset_sensor
from starlette import status
from toolz import get_in, merge

from nmdc_runtime.api.core.idgen import generate_id_unique
from nmdc_runtime.api.core.util import dotted_path_for
from nmdc_runtime.api.models.job import Job
from nmdc_runtime.api.models.operation import ObjectPutMetadata
from nmdc_runtime.pipelines.core import (
    preset_normal_env,
    run_config_frozen__preset_normal_env,
)
from nmdc_runtime.resources.core import get_runtime_api_site_client
from nmdc_runtime.resources.mongo import get_mongo


def asset_materialization_metadata(asset_event, key):
    for (
        e
    ) in (
        asset_event.dagster_event.step_materialization_data.materialization.metadata_entries
    ):
        if e.label == key:
            return e.entry_data
    return None


@asset_sensor(
    asset_key=AssetKey(["object", "nmdc_database.json.zip"]),
    pipeline_name="ensure_job_p",
    mode="normal",
)
def ensure_gold_translation_job(_context, asset_event):
    mdb = get_mongo(run_config=run_config_frozen__preset_normal_env).db
    gold_etl_latest = mdb.objects.find_one(
        {"name": "nmdc_database.json.zip"}, sort=[("created_time", -1)]
    )
    sensed_object_id = asset_materialization_metadata(asset_event, "object_id").text
    if gold_etl_latest is None:
        yield SkipReason("can't find sensed asset object_id in database")
        return
    elif gold_etl_latest["id"] != sensed_object_id:
        yield SkipReason("later object than sensed materialization")
        return

    run_config = merge(
        run_config_frozen__preset_normal_env,
        {
            "solids": {
                "ensure_job": {
                    "config": {
                        "job_base": {"workflow": {"id": "gold-translation-1.0.0"}},
                        "object_id_latest": gold_etl_latest["id"],
                    }
                }
            }
        },
    )
    yield RunRequest(run_key=sensed_object_id, run_config=run_config)


@asset_sensor(
    asset_key=AssetKey(["job", "gold-translation-1.0.0"]),
    pipeline_name="gold_translation_curation",
    mode="normal",
)
def claim_and_run_gold_translation_curation(_context, asset_event):
    # TODO test this sensor.
    client = get_runtime_api_site_client(
        run_config=run_config_frozen__preset_normal_env
    )
    mdb = get_mongo(run_config=run_config_frozen__preset_normal_env).db
    object_id_latest = asset_event.asset_key.object_id_latest
    job = mdb.jobs.find_one(
        {
            "workflow.id": "gold-translation-1.0.0",
            "config.object_id_latest": object_id_latest,
        }
    )
    if job is not None:
        rv = client.claim_job(job["id"])
        if rv.status_code == status.HTTP_200_OK:
            operation = rv.json()
            run_config = merge(
                run_config_frozen__preset_normal_env,
                {
                    "solids": {
                        "get_operation": {
                            "config": {
                                "operation_id": operation["id"],
                            }
                        }
                    }
                },
            )
            yield RunRequest(run_key=operation["id"], run_config=run_config)
        else:
            yield SkipReason("Job found, but already claimed by this site")
    else:
        yield SkipReason("No job found")
