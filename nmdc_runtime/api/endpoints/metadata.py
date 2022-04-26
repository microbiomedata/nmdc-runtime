import concurrent.futures
import json
import os.path
import re
import tempfile
from collections import defaultdict
from io import StringIO

import pandas as pd
import requests
from fastapi import APIRouter, UploadFile, File, HTTPException, Depends
from gridfs import GridFS
from jsonschema import Draft7Validator
from nmdc_schema.nmdc_data import get_nmdc_jsonschema_dict
from pymongo import ReturnDocument
from pymongo.database import Database as MongoDatabase
from starlette import status
from starlette.responses import StreamingResponse

from nmdc_runtime.api.core.metadata import (
    load_changesheet,
    update_mongo_db,
    mongo_update_command_for,
    copy_docs_in_update_cmd,
)
from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.endpoints.util import persist_content_and_get_drs_object
from nmdc_runtime.api.models.metadata import ChangesheetIn
from nmdc_runtime.api.models.object_type import DrsObjectWithTypes
from nmdc_runtime.api.models.user import User, get_current_active_user
from nmdc_runtime.site.drsobjects.registration import specialize_activity_set_docs

router = APIRouter()


async def raw_changesheet_from_uploaded_file(uploaded_file: UploadFile):
    content_type = uploaded_file.content_type
    name = uploaded_file.filename
    contents: bytes = await uploaded_file.read()
    text = contents.decode()
    return ChangesheetIn(name=name, content_type=content_type, text=text)


def df_from_sheet_in(sheet_in: ChangesheetIn, mdb: MongoDatabase) -> pd.DataFrame:
    content_types = {
        "text/csv": ",",
        "text/tab-separated-values": "\t",
    }
    content_type = sheet_in.content_type
    sep = content_types[content_type]
    filename = sheet_in.name
    if content_type not in content_types:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                f"file {filename} has content type '{content_type}'. "
                f"Only {list(content_types)} files are permitted."
            ),
        )
    try:
        df = load_changesheet(StringIO(sheet_in.text), mdb, sep=sep)
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    return df


def _validate_changesheet(df_change: pd.DataFrame, mdb: MongoDatabase):
    update_cmd = mongo_update_command_for(df_change)
    mdb_to_inspect = mdb.client["nmdc_changesheet_submission_results"]
    results_of_copy = copy_docs_in_update_cmd(
        update_cmd,
        mdb_from=mdb,
        mdb_to=mdb_to_inspect,
    )
    results_of_updates = update_mongo_db(mdb_to_inspect, update_cmd)
    rv = {
        "update_cmd": update_cmd,
        "inspection_info": {
            "mdb_name": mdb_to_inspect.name,
            "results_of_copy": results_of_copy,
        },
        "results_of_updates": results_of_updates,
    }
    for result in results_of_updates:
        if len(result.get("validation_errors", [])) > 0:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=rv
            )

    return rv


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
    allowed_to_submit = ("dehays", "dwinston", "pajau", "montana")
    if user.username not in allowed_to_submit:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=(
                f"Only users {allowed_to_submit} "
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
        id_ns="changesheets",
    )

    doc_after = mdb.objects.find_one_and_update(
        {"id": drs_obj_doc["id"]},
        {"$set": {"types": ["metadata-changesheet"]}},
        return_document=ReturnDocument.AFTER,
    )
    return doc_after


@router.get("/metadata/changesheets/{object_id}")
async def get_changesheet(
    object_id: str,
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    mdb_fs = GridFS(mdb)
    grid_out = mdb_fs.get(object_id)
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


# FIX (2021-12-16): this variable does not seem to be used anywhere else.
# Can it be deleted? Commenting out for now.
# type_collections = {
#     f'nmdc:{spec["items"]["$ref"].split("/")[-1]}': collection_name
#     for collection_name, spec in nmdc_jsonschema["properties"].items()
#     if collection_name.endswith("_set")
# }


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


def _validate_json(docs: dict):
    validator = Draft7Validator(get_nmdc_jsonschema_dict())
    docs, validation_errors = specialize_activity_set_docs(docs)

    for coll_name, coll_docs in docs.items():
        errors = list(validator.iter_errors({coll_name: coll_docs}))
        validation_errors[coll_name] = [e.message for e in errors]

    if all(len(v) == 0 for v in validation_errors.values()):
        return {"result": "All Okay!"}
    else:
        return {"result": "errors", "detail": validation_errors}


@router.post("/metadata/json:validate")
async def validate_json(docs: dict):
    """

    Validate a NMDC JSON Schema "nmdc:Database" object.

    """

    return _validate_json(docs)


@router.post("/metadata/json:submit")
async def submit_json(
    docs: dict,
    user: User = Depends(get_current_active_user),
):
    """

    Submit a NMDC JSON Schema "nmdc:Database" object.

    """
    raise HTTPException(
        status_code=status.HTTP_405_METHOD_NOT_ALLOWED, detail="not yet implemented"
    )

    # rv = _validate_json(docs)
    # if rv["result"] == "errors":
    #     raise HTTPException(
    #         status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
    #         detail=rv["validation_errors"],
    #     )
    # drs_obj_doc = persist_content_and_get_drs_object(
    #     content=json.dumps(docs),
    #     username=user.username,
    #     filename=None,
    #     content_type="application/json",
    #     id_ns="json-metadata-in",
    # )
    # dagster_job = repo.get_job("ensure_jobs")
    # # TODO ensure_jobs.to_job.execute_in_process(
    # #          {workflow.id=metadata-in-1.0.0,config.object_id=drs_obj_doc.id})
    # base_jobs = [
    #     {
    #         "workflow": {"id": "metadata-in-1.0.0"},
    #         "config": {"object_id": drs_obj_doc["id"]},
    #     }
    # ]
    # run_config = merge(
    #     run_config_frozen__normal_env,
    #     {"ops": {"construct_jobs": {"config": {"base_jobs": base_jobs}}}},
    # )
    # dagster_job.execute_in_process(run_config=unfreeze(run_config))
    #
    # # TODO get_runtime_api_site_client.claim_job,
    # #      dagster_client.submit_job_execution(apply_metadata_in.to_job)
    # # TODO return run_id of requested metadata-in job.
