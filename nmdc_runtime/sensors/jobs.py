import json

from dagster import RunRequest, sensor
from starlette import status
from toolz import get_in

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


@sensor(pipeline_name="gold_translation_curation", mode="normal")
def new_gold_translation(_context):
    client = get_runtime_api_site_client(
        run_config=run_config_frozen__preset_normal_env
    )
    mdb = get_mongo(run_config=run_config_frozen__preset_normal_env).db
    gold_etl_latest = mdb.objects.find_one(
        {"name": "nmdc_database.json.zip"}, sort=[("created_time", -1)]
    )
    if gold_etl_latest is None:
        return

    # TODO ensure job with id of latest object.
    #  if existing jobs for older objects, remove those jobs.
    jobs = list(mdb.jobs.find({"workflow.id": "gold-translation-1.0.0"}))
    jobs_older = [
        job
        for job in jobs
        if get_in(["config", "gold_etl_latest"], job) != gold_etl_latest["id"]
    ]
    if len(jobs_older):
        mdb.jobs.delete_many({"id": {"$in": [d["id"] for d in jobs_older]}})
    if len(jobs) == len(jobs_older):
        doc = {
            "id": generate_id_unique(mdb, "jobs"),
            "workflow": {"id": "gold-translation-1.0.0"},
            "config": {"gold_etl_latest": gold_etl_latest["id"]},
        }
        Job(**doc)
        mdb.jobs.insert_one(doc)

    job = mdb.jobs.find_one({"workflow.id": "gold-translation-1.0.0"})
    if job is not None:
        # TODO decouple periodic job creation from sensing to claim.
        #   Perhaps job creation (i.e. mdb.jobs updates) should be part of a *schedule*,
        #   whereas claiming + running a pipeline should be part of a *sensor*.
        rv = client.claim_job(job["id"])
        if rv.status_code == status.HTTP_200_OK:
            run_key = rv.json()["id"]  # operation id
            yield RunRequest(run_key=run_key, run_config=preset_normal_env.run_config)
