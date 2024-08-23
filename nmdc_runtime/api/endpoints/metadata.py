import concurrent.futures
import json
import os.path
import re
import tempfile
from collections import defaultdict
from copy import deepcopy
from io import StringIO

import requests
from dagster import ExecuteInProcessResult
from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from gridfs import GridFS, NoFile
from jsonschema import Draft7Validator
from nmdc_runtime.api.core.metadata import _validate_changesheet, df_from_sheet_in
from nmdc_runtime.api.core.util import API_SITE_CLIENT_ID
from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.endpoints.util import (
    _claim_job,
    _request_dagster_run,
    permitted,
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
    uploaded_file: UploadFile = File(...),
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    """

    Example changesheet
    [here](https://github.com/microbiomedata/nmdc-runtime/blob/main/metadata-translation/notebooks/data/changesheet-without-separator3.tsv).

    """
    sheet_in = await raw_changesheet_from_uploaded_file(uploaded_file)
    df_change = df_from_sheet_in(sheet_in, mdb)
    return _validate_changesheet(df_change, mdb)


@router.post("/metadata/changesheets:submit", response_model=DrsObjectWithTypes)
async def submit_changesheet(
    uploaded_file: UploadFile = File(...),
    mdb: MongoDatabase = Depends(get_mongo_db),
    user: User = Depends(get_current_active_user),
):
    """

    Example changesheet
    [here](https://github.com/microbiomedata/nmdc-runtime/blob/main/metadata-translation/notebooks/data/changesheet-without-separator3.tsv).

    """
    if not permitted(user.username, "/metadata/changesheets:submit"):
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


@router.get("/metadata/stored_files/{object_id}")
async def get_stored_metadata_object(
    object_id: str,
    mdb: MongoDatabase = Depends(get_mongo_db),
):
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


url_pattern = re.compile(r"https?://(?P<domain>[^/]+)/(?P<path>.+)")


def url_to_name(url):
    m = url_pattern.match(url)
    return f"{'.'.join(reversed(m.group('domain').split('.')))}__{m.group('path').replace('/', '.')}"


def result_for_url_to_json_file(data, url, save_dir):
    with open(os.path.join(save_dir, url_to_name(url)), "w") as f:
        json.dump(data.json(), f)


def fetch_downloaded_json(url, save_dir):
    with open(os.path.join(save_dir, url_to_name(url))) as f:
        return json.load(f)


@router.post("/metadata/json:validate_urls_file")
async def validate_json_urls_file(urls_file: UploadFile = File(...)):
    """

    Given a text file with one URL per line, will try to validate each URL target
    as a NMDC JSON Schema "nmdc:Database" object.

    """
    content_type = urls_file.content_type
    filename = urls_file.filename
    if content_type != "text/plain":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                f"file {filename} has content type '{content_type}'. "
                f"Only 'text/plain' (*.txt) files are permitted."
            ),
        )
    contents: bytes = await urls_file.read()
    stream = StringIO(contents.decode())  # can e.g. import csv; csv.reader(stream)

    urls = [line.strip() for line in stream if line.strip()]

    def load_url(url, timeout):
        return requests.get(url, timeout=timeout)

    with tempfile.TemporaryDirectory() as temp_dir:
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            future_to_url = {executor.submit(load_url, url, 5): url for url in urls}
            for future in concurrent.futures.as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    data = future.result()
                    result_for_url_to_json_file(data, url, temp_dir)
                except Exception as exc:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"{url} generated an exception: {exc}",
                    )

        validator = Draft7Validator(get_nmdc_jsonschema_dict())
        validation_errors = defaultdict(list)

        for url in urls:
            docs = fetch_downloaded_json(url, temp_dir)
            docs, validation_errors_for_activity_set = specialize_activity_set_docs(
                docs
            )

            validation_errors["activity_set"].extend(
                validation_errors_for_activity_set["activity_set"]
            )

            for coll_name, coll_docs in docs.items():
                errors = list(validator.iter_errors({coll_name: coll_docs}))
                validation_errors[coll_name].extend([e.message for e in errors])

        if all(len(v) == 0 for v in validation_errors.values()):
            return {"result": "All Okay!"}
        else:
            return {"result": "errors", "detail": validation_errors}


@router.post("/metadata/json:validate", name="Validate JSON")
async def validate_json_nmdcdb(docs: dict, mdb: MongoDatabase = Depends(get_mongo_db)):
    """

    Validate a NMDC JSON Schema "nmdc:Database" object.

    """

    return validate_json(docs, mdb)


@router.post("/metadata/json:submit", name="Submit JSON")
async def submit_json_nmdcdb(
    docs: dict,
    user: User = Depends(get_current_active_user),
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    """

    Submit a NMDC JSON Schema "nmdc:Database" object.

    """
    if not permitted(user.username, "/metadata/json:submit"):
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
