import json
from typing import Any, Dict, List

from fastapi import APIRouter, HTTPException, Depends, Response, status
from pydantic import ValidationError
from dagster import ExecuteInProcessResult
from bson import json_util
from pymongo import ReturnDocument
from toolz import merge
from pymongo.errors import DuplicateKeyError
from pymongo.database import Database as MongoDatabase

from nmdc_runtime.api.models.user import User, get_current_active_user
from nmdc_runtime.util import unfreeze

from .models.ingest import Ingest

from nmdc_runtime.api.endpoints.util import (
    persist_content_and_get_drs_object,
    _claim_job,
    _request_dagster_run,
    permitted,
    users_allowed,
)
from nmdc_runtime.site.repository import run_config_frozen__normal_env, repo
from components.workflow.workflow.core import (
    DataObjectService,
    ReadsQCSequencingActivityService,
)
from ..db.mongo import get_mongo_db
from ..models.object_type import DrsObjectWithTypes

router = APIRouter(prefix="/outputs", tags=["outputs"])


@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    response_model=DrsObjectWithTypes,
)
async def ingest(
    ingest: Ingest,
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    try:
        drs_obj_doc = persist_content_and_get_drs_object(
            content=ingest.json(),
            filename=None,
            content_type="application/json",
            description="input metadata for readqc-in wf",
            id_ns="json-readqc-in",
        )
        mgs_service = ReadsQCSequencingActivityService()
        data_object_service = DataObjectService()
        object_dict = [member.dict() for member in ingest.data_object_set]
        activity_dict = [
            member.dict() for member in ingest.read_QC_analysis_activity_set
        ]
        _ = [
            await data_object_service.create_data_object(data_object)
            for data_object in object_dict
        ]
        _ = [
            await mgs_service.create_mgs_activity(activity)
            for activity in activity_dict
        ]

        doc_after = mdb.objects.find_one_and_update(
            {"id": drs_obj_doc["id"]},
            {"$set": {"types": ["readqc-in"]}},
            return_document=ReturnDocument.AFTER,
        )
        return doc_after

    except DuplicateKeyError as e:
        raise HTTPException(status_code=409, detail=e.details)
