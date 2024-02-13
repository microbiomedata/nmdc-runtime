"""Module."""
import os
from typing import Any

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo.database import Database as MongoDatabase
from pymongo.errors import BulkWriteError
from starlette import status

from components.nmdc_runtime.workflow_execution_activity import (
    ActivityService,
    Database,
)
from nmdc_runtime.api.db.mongo import get_async_mongo_db, get_mongo_db
from nmdc_runtime.api.models.site import Site, get_current_client_site
from nmdc_runtime.site.resources import MongoDB
from nmdc_runtime.util import validate_json

router = APIRouter(
    prefix="/workflows/activities", tags=["workflow_execution_activities"]
)


async def job_to_db(job_spec: dict[str, Any], mdb: AsyncIOMotorDatabase) -> None:
    return await mdb["jobs"].insert_one(job_spec)


@router.post("", status_code=status.HTTP_201_CREATED)
async def post_activity(
    activity_set: dict[str, Any],
    site: Site = Depends(get_current_client_site),
    mdb: MongoDatabase = Depends(get_mongo_db),
) -> dict[str, str]:
    """Post activity set to database and claim job.

    Parameters
    ----------
    activity_set : dict[str,Any]
        Set of activities for specific workflows.

    Returns
    -------
    dict[str,str]
    """
    _ = site  # must be authenticated
    try:
        if "activity_set" not in activity_set:
            raise ValueError("need to supply `activity_set`")
        activity_set = {"activity_set": activity_set["activity_set"]}
        rv = validate_json(activity_set, mdb)
        if rv["result"] == "errors":
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=str(rv),
            )
        mongo_resource = MongoDB(
            host=os.getenv("MONGO_HOST"),
            dbname=os.getenv("MONGO_DBNAME"),
            username=os.getenv("MONGO_USERNAME"),
            password=os.getenv("MONGO_PASSWORD"),
        )
        mongo_resource.add_docs(activity_set, validate=False, replace=True)
        return {"message": "jobs accepted"}
    except BulkWriteError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))
