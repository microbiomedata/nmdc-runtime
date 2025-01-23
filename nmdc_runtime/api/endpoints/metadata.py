import concurrent.futures
import json
import os.path
import re
import tempfile
from collections import defaultdict
from copy import deepcopy
from io import StringIO
from typing import Annotated

import requests
from dagster import ExecuteInProcessResult
from fastapi import APIRouter, Depends, File, HTTPException, UploadFile, Path
from gridfs import GridFS, NoFile
from jsonschema import Draft7Validator
from nmdc_runtime.api.core.metadata import _validate_changesheet, df_from_sheet_in
from nmdc_runtime.api.core.util import API_SITE_CLIENT_ID
from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.endpoints.util import (
    _claim_job,
    _request_dagster_run,
    check_action_permitted,
    persist_content_and_get_drs_object,
)
from nmdc_runtime.api.models.job import Job
from nmdc_runtime.api.models.metadata import ChangesheetIn
from nmdc_runtime.api.models.object_type import DrsObjectWithTypes
from nmdc_runtime.api.models.site import get_site
from nmdc_runtime.api.models.user import User, get_current_active_user
from nmdc_runtime.site.repository import repo, run_config_frozen__normal_env
from nmdc_runtime.util import (
    unfreeze,
    validate_json,
    specialize_activity_set_docs,
)
from nmdc_runtime.util import get_nmdc_jsonschema_dict
from pymongo import ReturnDocument
from pymongo.database import Database as MongoDatabase
from starlette import status
from starlette.responses import StreamingResponse
from toolz import merge

router = APIRouter()


async def raw_changesheet_from_uploaded_file(uploaded_file: UploadFile):
    """
    Extract utf8-encoded text from fastapi.UploadFile object, and
    construct ChangesheetIn object for subsequent processing.
    """
    content_type = uploaded_file.content_type
    name = uploaded_file.filename
    if name.endswith(".csv"):
        content_type = "text/csv"
    elif name.endswith(".tsv"):
        content_type = "text/tab-separated-values"
    contents: bytes = await uploaded_file.read()
    text = contents.decode()
    return ChangesheetIn(name=name, content_type=content_type, text=text)


@router.post("/metadata/changesheets:validate")
async def validate_changesheet(
    uploaded_file: UploadFile = File(
        ..., description="The changesheet you want the server to validate"
    ),
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    r"""
    Validates a [changesheet](https://microbiomedata.github.io/nmdc-runtime/howto-guides/author-changesheets/)
    that is in either CSV or TSV format.
    """
    sheet_in = await raw_changesheet_from_uploaded_file(uploaded_file)
    df_change = df_from_sheet_in(sheet_in, mdb)
    return _validate_changesheet(df_change, mdb)


@router.post("/metadata/changesheets:submit", response_model=DrsObjectWithTypes)
async def submit_changesheet(
    uploaded_file: UploadFile = File(
        ..., description="The changesheet you want the server to apply"
    ),
    mdb: MongoDatabase = Depends(get_mongo_db),
    user: User = Depends(get_current_active_user),
):
    r"""
    Applies a [changesheet](https://microbiomedata.github.io/nmdc-runtime/howto-guides/author-changesheets/)
    that is in either CSV or TSV format.

    **Note:** This endpoint is only accessible to users that have been granted access by a Runtime administrator.
    """
    # TODO: Allow users to determine whether they have that access (i.e. whether they are allowed to perform the
    #       `/metadata/changesheets:submit` action), themselves, so that they don't have to contact an admin
    #       or submit an example changesheet in order to find that out.

    if not check_action_permitted(user.username, "/metadata/changesheets:submit"):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=(
                f"Only specific users "
                "are allowed to apply changesheets at this time."
            ),
        )
    sheet_in = await raw_changesheet_from_uploaded_file(uploaded_file)
    df_change = df_from_sheet_in(sheet_in, mdb)
    _ = _validate_changesheet(df_change, mdb)

    drs_obj_doc = persist_content_and_get_drs_object(
        content=sheet_in.text,
        username=user.username,
        filename=re.sub(r"[^A-Za-z0-9._\-]", "_", sheet_in.name),
        content_type=sheet_in.content_type,
        description="changesheet",
        id_ns="changesheets",
    )

    doc_after = mdb.objects.find_one_and_update(
        {"id": drs_obj_doc["id"]},
        {"$set": {"types": ["metadata-changesheet"]}},
        return_document=ReturnDocument.AFTER,
    )
    return doc_after


@router.get("/metadata/stored_files/{object_id}", include_in_schema=False)
async def get_stored_metadata_object(
    object_id: Annotated[
        str,
        Path(
            title="Metadata file ObjectId",
            description="The ObjectId (`_id`) of the metadata file you want to get.\n\n_Example_: `507f1f77bcf86cd799439011`",
            examples=["507f1f77bcf86cd799439011"],
        ),
    ],
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    r"""
    This endpoint is subservient to our Data Repository Service (DRS) implementation, i.e. the `/objects/*` endpoints.
    In particular, URLs resolving to this route are generated
    by the DRS `/objects/{object_id}/access/{access_id}` endpoint if we store the raw object in our MongoDB via GridFS.
    We currently do this for request bodies for `/metadata/json:submit` and `/metadata/changesheets:submit`.
    A typical API user would not call this endpoint directly. Rather, it merely forms part of the API surface.
    Therefore, we do not include it in the OpenAPI schema.

    References:
    - https://pymongo.readthedocs.io/en/stable/examples/gridfs.html
    - https://www.mongodb.com/docs/manual/core/gridfs/#use-gridfs
    """
    mdb_fs = GridFS(mdb)
    try:
        grid_out = mdb_fs.get(object_id)
    except NoFile:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Metadata stored file {object_id} not found",
        )
    filename, content_type = grid_out.filename, grid_out.content_type

    def iter_grid_out():
        yield from grid_out

    return StreamingResponse(
        iter_grid_out(),
        media_type=content_type,
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@router.post("/metadata/json:validate", name="Validate JSON")
async def validate_json_nmdcdb(docs: dict, mdb: MongoDatabase = Depends(get_mongo_db)):
    r"""
    Validate a NMDC JSON Schema "nmdc:Database" object.

    This API endpoint validates the JSON payload in two steps. The first step is to check the format of each document
    (e.g., the presence, name, and value of each field). If it encounters any violations during that step, it will not
    proceed to the second step. The second step is to check whether all documents referenced by the document exist,
    whether in the database or the same JSON payload. We call the second step a "referential integrity check."
    """

    return validate_json(docs, mdb, check_inter_document_references=True)


@router.post("/metadata/json:submit", name="Submit JSON")
async def submit_json_nmdcdb(
    docs: dict,
    user: User = Depends(get_current_active_user),
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    """

    Submit a NMDC JSON Schema "nmdc:Database" object.

    """
    if not check_action_permitted(user.username, "/metadata/json:submit"):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only specific users are allowed to submit json at this time.",
        )
    rv = validate_json(docs, mdb)
    if rv["result"] == "errors":
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(rv),
        )

    extra_run_config_data = _ensure_job__metadata_in(docs, user.username, mdb)

    requested = _request_dagster_run(
        nmdc_workflow_id="metadata-in-1.0.0",
        nmdc_workflow_inputs=[],  # handled by _request_dagster_run given extra_run_config_data
        extra_run_config_data=extra_run_config_data,
        mdb=mdb,
        user=user,
    )
    if requested["type"] == "success":
        return requested
    else:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=(
                f"Runtime failed to start metadata-in-1.0.0 job. "
                f'Detail: {requested["detail"]}'
            ),
        )


def _ensure_job__metadata_in(
    docs, username, mdb, client_id=API_SITE_CLIENT_ID, drs_object_exists_ok=False
):
    drs_obj_doc = persist_content_and_get_drs_object(
        content=json.dumps(docs),
        username=username,
        filename=None,
        content_type="application/json",
        description="JSON metadata in",
        id_ns="json-metadata-in",
        exists_ok=drs_object_exists_ok,
    )
    job_spec = {
        "workflow": {"id": "metadata-in-1.0.0"},
        "config": {"object_id": drs_obj_doc["id"]},
    }
    run_config = merge(
        unfreeze(run_config_frozen__normal_env),
        {"ops": {"construct_jobs": {"config": {"base_jobs": [job_spec]}}}},
    )
    dagster_result: ExecuteInProcessResult = repo.get_job(
        "ensure_jobs"
    ).execute_in_process(run_config=run_config)
    job = Job(**mdb.jobs.find_one(job_spec))
    if not dagster_result.success or job is None:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f'failed to complete metadata-in-1.0.0/{drs_obj_doc["id"]} job',
        )

    site = get_site(mdb, client_id=client_id)
    operation = _claim_job(job.id, mdb, site)
    return {
        "ops": {
            "get_json_in": {
                "config": {
                    "object_id": job.config.get("object_id"),
                }
            },
            "perform_mongo_updates": {"config": {"operation_id": operation["id"]}},
        }
    }
