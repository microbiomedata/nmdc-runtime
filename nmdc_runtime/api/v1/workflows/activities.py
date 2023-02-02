"""Module."""
import json
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo.database import Database as MongoDatabase
from starlette import status
from toolz import merge

from components.nmdc_runtime.workflow_execution_activity import (
    ActivityService, Database)
from nmdc_runtime.api.db.mongo import get_async_mongo_db, get_mongo_db
from nmdc_runtime.api.endpoints.util import persist_content_and_get_drs_object
from nmdc_runtime.api.models.site import Site, get_current_client_site
from nmdc_runtime.site.repository import repo, run_config_frozen__normal_env
from nmdc_runtime.util import unfreeze

router = APIRouter(
    prefix="/workflows/activities", tags=["workflow_execution_activities"]
)


@router.post("", status_code=status.HTTP_201_CREATED)
async def post_activity(
    activity_set: dict,
    site: Site = Depends(get_current_client_site),
    mdb: MongoDatabase = Depends(get_mongo_db),
    amdb: AsyncIOMotorDatabase = Depends(get_async_mongo_db),
) -> None:
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
        # job_configs = activity_service.create_jobs(activities, nmdc_db.data_object_set)

        # drs_obj_doc: dict[str, Any] = persist_content_and_get_drs_object(
        #     content=json.dumps(
        #         [
        #             json.loads(json.dumps(do, default=lambda o: o.__dict__))
        #             for do in nmdc_db.data_object_set
        #         ]
        #     ),
        #     filename="foo.json",
        #     content_type="application/json",
        #     description="input metadata for wf",
        #     id_ns="json-metadata-in-1.0.1",
        # )
        # for job in job_configs:
        #     job_spec = {
        #         "workflow": {"id": f"{job['id_type']}-{job['version']}"},
        #         "config": {"object_id": drs_obj_doc["id"], **job},
        #         "claims": [],
        #     }

        #     run_config = merge(
        #         unfreeze(run_config_frozen__normal_env),
        #         {"ops": {"construct_jobs": {"config": {"base_jobs": [job_spec]}}}},
        #     )

        #     repo.get_job("ensure_jobs").execute_in_process(run_config=run_config)
        # job_ids: list[dict[str, Any]] = (
        #     await amdb["jobs"]
        #     .find({"config.object_id": drs_obj_doc["id"]}, {"id": 1})
        #     .to_list(100)
        # )

    except Exception as e:
        print(e)
        raise HTTPException(status_code=409, detail=e.details)
