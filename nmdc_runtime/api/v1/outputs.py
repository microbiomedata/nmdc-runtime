"""Beans."""
import json
from typing import Any

from bson import json_util
from dagster import ExecuteInProcessResult
from fastapi import APIRouter, Depends, HTTPException
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

from ..db.mongo import get_mongo_db
from ..models.object_type import DrsObjectWithTypes
from .models.ingest import Ingest

router = APIRouter(prefix="/outputs", tags=["outputs"])


@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    response_model=dict[str, Any],
)
async def ingest(
    ingest: Ingest,
    mdb: MongoDatabase = Depends(get_mongo_db),
    site: Site = Depends(get_current_client_site),
):
    """Ingest activity set."""
    try:

        if site is None:
            raise HTTPException(status_code=401, detail="Client site not found")

        drs_obj_doc = persist_content_and_get_drs_object(
            content=ingest.json(),
            filename=None,
            content_type="application/json",
            description="input metadata for readqc-in wf",
            id_ns="json-readqc-in",
        )

        job_spec = {
            "workflow": {"id": "readqc-1.0.1"},
            "config": {"object_id": drs_obj_doc["id"]},
        }

        run_config = merge(
            unfreeze(run_config_frozen__normal_env),
            {"ops": {"construct_jobs": {"config": {"base_jobs": [job_spec]}}}},
        )

        dagster_result: ExecuteInProcessResult = repo.get_job(
            "ensure_jobs"
        ).execute_in_process(run_config=run_config)

        return drs_obj_doc

    except DuplicateKeyError as e:
        raise HTTPException(status_code=409, detail=e.details)
