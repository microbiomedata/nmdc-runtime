"""Module."""
import json
from dataclasses import asdict

from components.nmdc_runtime.workflow_execution_activity import (
    ActivityService, Database, init_activity_service)
from dagster import ExecuteInProcessResult
from fastapi import APIRouter, Depends, HTTPException
from nmdc_runtime.api.core.idgen import generate_one_id, local_part
from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.endpoints.util import (
    _claim_job, _request_dagster_run, permitted,
    persist_content_and_get_drs_object, users_allowed)
from nmdc_runtime.api.models.site import Site, get_current_client_site
from nmdc_runtime.site.repository import repo, run_config_frozen__normal_env
from nmdc_runtime.util import unfreeze
from pymongo import ReturnDocument
from pymongo.database import Database as MongoDatabase
from pymongo.errors import DuplicateKeyError
from starlette import status
from toolz import merge

router = APIRouter(
    prefix="/workflows/activities", tags=["workflow_execution_activities"]
)


@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    response_model=list[str],
)
async def post_activity(
    activity_set: dict,
    # site: Site = Depends(get_current_client_site),
    mdb: MongoDatabase = Depends(get_mongo_db),
) -> bool:
    """Post activity set to database and claim job.

    Parameters
    ----------
    activity_set : ActivitySet
                   Set of activities for specific workflows.
    """
    try:
        activity_service = ActivityService()

        nmdc_db = Database(**activity_set)
        activities = await activity_service.add_activity_set(nmdc_db, mdb)
        job_configs = activity_service.create_jobs(activities, nmdc_db.data_object_set)

        drs_obj_doc = persist_content_and_get_drs_object(
            content=json.dumps(
                [
                    json.loads(json.dumps(do, default=lambda o: o.__dict__))
                    for do in nmdc_db.data_object_set
                ]
            ),
            filename=None,
            content_type="application/json",
            description=f"input metadata for wf",
            id_ns=f"json-metadata-in-1.0.1",
        )
        for job in job_configs:
            job_spec = {
                "workflow": {"id": "automation-in-1.0.0"},
                "config": {"object_id": drs_obj_doc["id"], **job},
            }

            run_config = merge(
                unfreeze(run_config_frozen__normal_env),
                {"ops": {"construct_jobs": {"config": {"base_jobs": [job_spec]}}}},
            )

            dagster_result: ExecuteInProcessResult = repo.get_job(
                "ensure_jobs"
            ).execute_in_process(run_config=run_config)

        return [job["Inputs"]["proj"] for job in job_configs]

    except DuplicateKeyError as e:
        raise HTTPException(status_code=409, detail=e.details)
