"""Module."""
from typing import Any
from uuid import uuid1

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo.database import Database as MongoDatabase
from starlette import status

from components.nmdc_runtime.workflow_execution_activity import (
    ActivityService,
    Database,
)
from nmdc_runtime.api.db.mongo import get_async_mongo_db, get_mongo_db
from nmdc_runtime.api.models.site import Site, get_current_client_site

router = APIRouter(
    prefix="/workflows/activities", tags=["workflow_execution_activities"]
)


async def job_to_db(job_spec: dict[str, Any], mdb: AsyncIOMotorDatabase) -> None:
    return await mdb["jobs"].insert_one(job_spec)


@router.post("", status_code=status.HTTP_201_CREATED)
async def post_activity(
    activity_set: dict[str, Any],
    background_tasks: BackgroundTasks,
    site: Site = Depends(get_current_client_site),
    mdb: MongoDatabase = Depends(get_mongo_db),
    amdb: AsyncIOMotorDatabase = Depends(get_async_mongo_db),
) -> dict[str, str]:
    """Post activity set to database and claim job.

    Parameters
    ----------
    activity_set : dict[str, Any]
                   Set of activities for specific workflows.

    Returns
    _______
    dict[str, str]
    """
    try:
        activity_service = ActivityService()
        nmdc_db = Database(**activity_set)
        activities = await activity_service.add_activity_set(nmdc_db, mdb)
        job_configs = activity_service.create_jobs(activities, nmdc_db.data_object_set)
        for job in job_configs:
            job_spec = {
                "id": f"sys:test_{str(uuid1())}",
                "workflow": {"id": f"{job['id_type']}-{job['version']}"},
                "config": {**job},
                "claims": [],
            }
            background_tasks.add_task(job_to_db, job_spec, amdb)
        return {"message": "jobs accepted"}

    except Exception as e:
        print(e)
        raise HTTPException(status_code=409, detail=e)
