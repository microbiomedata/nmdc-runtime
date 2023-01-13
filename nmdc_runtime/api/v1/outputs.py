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

        # if site is None:
        #     raise HTTPException(status_code=401, detail="Client site not found")
        input_dict = {
            "readqc-in": ["mgasmb", "rba"],
            "mgasmb-in": ["mganno"],
            "mganno-in": ["mgasmbgen"],
        }

        metadata_type = None
        analysis_set = []

        if ingest.read_qc_analysis_activity_set:
            metadata_type = "readqc-in"
            analysis_set = ingest.read_qc_analysis_activity_set

        if ingest.metagenome_assembly_activity_set:
            metadata_type = "mgasmb-in"
            analysis_set = ingest.metagenome_assembly_activity_set

        if ingest.metagenome_annotation_activity_set:
            metadata_type = "mganno-in"
            analysis_set = ingest.metagenome_annotation_activity_set

        drs_obj_doc = persist_content_and_get_drs_object(
            content=ingest.json(),
            filename="something.json",
            content_type="application/json",
            description=f"input metadata for {metadata_type} wf",
            id_ns=f"json-{input_dict[metadata_type]}-1.0.1",
        )

        for workflow_job in input_dict[metadata_type]:
            job_spec = {
                "workflow": {"id": f"{workflow_job}-1.0.1"},
                "config": {
                    "object_id": drs_obj_doc["id"],
                    "activity_id": analysis_set[0].id,
                    "git_repo": analysis_set[0].git_url,
                    "was_informed_by": analysis_set[0].was_informed_by,
                    "type": analysis_set[0].type,
                    "inputs": ingest.data_object_set,
                    "started_at_time": analysis_set[0].started_at_time,
                    "ended_at_time": analysis_set[0].ended_at_time,
                },
            }

            run_config = merge(
                unfreeze(run_config_frozen__normal_env),
                {"ops": {"construct_jobs": {"config": {"base_jobs": [job_spec]}}}},
            )

            dagster_result: ExecuteInProcessResult = repo.get_job(
                "ensure_jobs"
            ).execute_in_process(run_config=run_config)

        doc_after = mdb.objects.find_one_and_update(
            {"id": drs_obj_doc["id"]},
            {"$set": {"types": [metadata_type]}},
            return_document=ReturnDocument.AFTER,
        )
        return json.loads(json_util.dumps(doc_after))

    except DuplicateKeyError as e:
        raise HTTPException(status_code=409, detail=e.details)
