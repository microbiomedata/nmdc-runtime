import logging
import os
import re
import tempfile
from pathlib import Path
from time import time_ns
from typing import Set, Optional, List, Tuple
from urllib.parse import urlparse, parse_qs

from bson import json_util
from dagster import PipelineRunStatus
from dagster_graphql import DagsterGraphQLClientError
from fastapi import HTTPException
from gridfs import GridFS
from pymongo.collection import Collection as MongoCollection
from pymongo.database import Database as MongoDatabase
from pymongo.errors import DuplicateKeyError
from starlette import status
from toolz import merge, dissoc, concat

from nmdc_runtime.api.core.idgen import generate_one_id, local_part
from nmdc_runtime.api.core.util import (
    raise404_if_none,
    expiry_dt_from_now,
    dotted_path_for,
)
from nmdc_runtime.api.db.mongo import activity_collection_names, get_mongo_db
from nmdc_runtime.api.models.job import Job, JobClaim, JobOperationMetadata
from nmdc_runtime.api.models.object import (
    PortableFilename,
    DrsId,
    DrsObjectIn,
    DrsObject,
)
from nmdc_runtime.api.models.operation import Operation
from nmdc_runtime.api.models.run import get_dagster_graphql_client
from nmdc_runtime.api.models.site import Site
from nmdc_runtime.api.models.util import (
    ListRequest,
    FindRequest,
    PipelineFindRequest,
    PipelineFindResponse,
    FindResponse,
    ResultT,
)
from nmdc_runtime.util import drs_metadata_for

BASE_URL_INTERNAL = os.getenv("API_HOST")
BASE_URL_EXTERNAL = os.getenv("API_HOST_EXTERNAL")
HOSTNAME_EXTERNAL = BASE_URL_EXTERNAL.split("://", 1)[-1]


def list_resources(req: ListRequest, mdb: MongoDatabase, collection_name: str):
    limit = req.max_page_size
    filter_ = json_util.loads(req.filter) if req.filter else {}
    if req.page_token:
        doc = mdb.page_tokens.find_one({"_id": req.page_token, "ns": collection_name})
        if doc is None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail="Bad page_token"
            )
        last_id = doc["last_id"]
        mdb.page_tokens.delete_one({"_id": req.page_token})
    else:
        last_id = None
    if last_id is not None:
        if "id" in filter_:
            filter_["id"] = merge(filter_["id"], {"$gt": last_id})
        else:
            filter_ = merge(filter_, {"id": {"$gt": last_id}})

    if mdb[collection_name].count_documents(filter=filter_) <= limit:
        rv = {"resources": list(mdb[collection_name].find(filter=filter_))}
        return rv
    else:
        if "id_1" not in mdb[collection_name].index_information():
            logging.warning(
                f"list_resources: no index set on 'id' for collection {collection_name}"
            )
        resources = list(
            mdb[collection_name].find(filter=filter_, limit=limit, sort=[("id", 1)])
        )
        last_id = resources[-1]["id"]
        token = generate_one_id(mdb, "page_tokens")
        mdb.page_tokens.insert_one(
            {"_id": token, "ns": collection_name, "last_id": last_id}
        )
        return {"resources": resources, "next_page_token": token}


def maybe_unstring(val):
    try:
        return float(val)
    except ValueError:
        return val


def get_pairs(s):
    return re.split(r"\s*,\s*", s)  # comma, perhaps surrounded by whitespace


def get_mongo_filter(filter_str):
    filter_ = {}
    if not filter_str:
        return filter_

    pairs = get_pairs(filter_str)
    if not all(len(split) == 2 for split in (p.split(":", maxsplit=1) for p in pairs)):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Filter must be of form: attribute:spec[,attribute:spec]*",
        )

    for attr, spec in (p.split(":", maxsplit=1) for p in pairs):
        if attr.endswith(".search"):
            actual_attr = attr[: -len(".search")]
            filter_[actual_attr] = {"$regex": spec}
        else:
            for op, key in {("<", "$lt"), ("<=", "$lte"), (">", "$gt"), (">=", "$gte")}:
                if spec.startswith(op):
                    filter_[attr] = {key: maybe_unstring(spec[len(op) :])}
                    break
            else:
                filter_[attr] = spec
    return filter_


def get_mongo_sort(sort_str) -> Optional[List[Tuple[str, int]]]:
    sort_ = []
    if not sort_str:
        return None

    pairs = get_pairs(sort_str)
    for p in pairs:
        components = p.split(":", maxsplit=1)
        if len(components) == 1:
            attr, spec = components[0], ""
        else:
            attr, spec = components
        for op, key in {("", 1), ("asc", 1), ("desc", -1)}:
            if spec == op:
                sort_.append((attr, key))
                break
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=(
                    "Sort must be of form: attribute:spec[,attribute:spec]* "
                    "where spec is `asc` (ascending -- the default if no spec) "
                    "or `desc` (descending).",
                ),
            )
    return sort_


def strip_oid(doc):
    return dissoc(doc, "_id")


def timeit(cursor):
    """Collect from cursor and return time taken in milliseconds."""
    tic = time_ns()
    results = list(cursor)
    toc = time_ns()
    return results, int(round((toc - tic) / 1e6))


def find_resources(req: FindRequest, mdb: MongoDatabase, collection_name: str):
    if req.group_by:
        raise HTTPException(
            status_code=status.HTTP_418_IM_A_TEAPOT,
            detail="I don't yet know how to ?group_by=",
        )
    if req.search:
        raise HTTPException(
            status_code=status.HTTP_418_IM_A_TEAPOT,
            detail=(
                "I don't yet know how to ?search=. "
                "Use ?filter=<attribute>.search:<spec> instead."
            ),
        )

    filter_ = get_mongo_filter(req.filter)
    sort_ = get_mongo_sort(req.sort)

    total_count = mdb[collection_name].count_documents(filter=filter_)

    if req.page:
        skip = (req.page - 1) * req.per_page
        if skip > 10_000:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Use cursor-based pagination for paging beyond 10,000 items",
            )
        limit = req.per_page
        results, db_response_time_ms = timeit(
            mdb[collection_name].find(
                filter=filter_, skip=skip, limit=limit, sort=sort_
            )
        )
        rv = {
            "meta": {
                "mongo_filter_dict": filter_,
                "mongo_sort_list": [[a, s] for a, s in sort_] if sort_ else None,
                "count": total_count,
                "db_response_time_ms": db_response_time_ms,
                "page": req.page,
                "per_page": req.per_page,
            },
            "results": [strip_oid(d) for d in results],
            "group_by": [],
        }

    else:  # req.cursor is not None
        if req.cursor != "*":
            doc = mdb.page_tokens.find_one({"_id": req.cursor, "ns": collection_name})
            if doc is None:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST, detail="Bad cursor value"
                )
            last_id = doc["last_id"]
            mdb.page_tokens.delete_one({"_id": req.cursor})
        else:
            last_id = None

        if last_id is not None:
            if "id" in filter_:
                filter_["id"] = merge(filter_["id"], {"$gt": last_id})
            else:
                filter_ = merge(filter_, {"id": {"$gt": last_id}})

        if "id_1" not in mdb[collection_name].index_information():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Cursor-based pagination is not enabled for this resource.",
            )

        limit = req.per_page
        sort_for_cursor = (sort_ or []) + [("id", 1)]
        results, db_response_time_ms = timeit(
            mdb[collection_name].find(filter=filter_, limit=limit, sort=sort_for_cursor)
        )
        last_id = results[-1]["id"]

        # Is this the last id overall? Then next_cursor should be None.
        filter_eager = filter_
        if "id" in filter_:
            filter_eager["id"] = merge(filter_["id"], {"$gt": last_id})
        else:
            filter_eager = merge(filter_, {"id": {"$gt": last_id}})
        more_results = (
            mdb[collection_name].count_documents(filter=filter_eager, limit=limit) > 0
        )
        if more_results:
            token = generate_one_id(mdb, "page_tokens")
            mdb.page_tokens.insert_one(
                {"_id": token, "ns": collection_name, "last_id": last_id}
            )
        else:
            token = None

        rv = {
            "meta": {
                "mongo_filter_dict": filter_,
                "mongo_sort_list": sort_for_cursor,
                "count": total_count,
                "db_response_time_ms": db_response_time_ms,
                "page": None,
                "per_page": req.per_page,
                "next_cursor": token,
            },
            "results": [strip_oid(d) for d in results],
            "group_by": [],
        }
    return rv


def find_resources_spanning(
    req: FindRequest, mdb: MongoDatabase, collection_names: Set[str]
):
    if req.cursor or not req.page:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="This resource only supports page-based pagination",
        )

    responses = {name: find_resources(req, mdb, name) for name in collection_names}
    rv = {
        "meta": {
            "mongo_filter_dict": next(
                r["meta"]["mongo_filter_dict"] for r in responses.values()
            ),
            "count": sum(r["meta"]["count"] for r in responses.values()),
            "db_response_time_ms": sum(
                r["meta"]["db_response_time_ms"] for r in responses.values()
            ),
            "page": req.page,
            "per_page": req.per_page,
        },
        "results": list(concat(r["results"] for r in responses.values())),
        "group_by": [],
    }
    return rv


def exists(collection: MongoCollection, filter_: dict):
    return collection.count_documents(filter_) > 0


def find_for(resource: str, req: FindRequest, mdb: MongoDatabase):
    if resource == "biosamples":
        return find_resources(req, mdb, "biosample_set")
    elif resource == "studies":
        return find_resources(req, mdb, "study_set")
    elif resource == "data_objects":
        return find_resources(req, mdb, "data_object_set")
    elif resource == "activities":
        return find_resources_spanning(req, mdb, activity_collection_names(mdb))
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                f"Unknown API resource '{resource}'. "
                f"Known resources: {{activities, biosamples, data_objects, studies}}."
            ),
        )


def pipeline_find_resources(req: PipelineFindRequest, mdb: MongoDatabase):
    description = req.description
    components = [c.strip() for c in re.split(r"\s*\n\s*\n\s*", req.pipeline_spec)]
    print(components)
    for c in components:
        if c.startswith("/"):
            parse_result = urlparse(c)
            resource = parse_result.path[1:]
            request_params_dict = {
                p: v[0] for p, v in parse_qs(parse_result.query).items()
            }
            req = FindRequest(**request_params_dict)
            resp = FindResponse(**find_for(resource, req, mdb))
            break
    components = [
        "NOTE: This method is yet to be implemented! Only the first stage is run!"
    ] + components
    return PipelineFindResponse(
        meta=merge(resp.meta, {"description": description, "components": components}),
        results=resp.results,
    )


def persist_content_and_get_drs_object(
    content: str,
    username="(anonymous)",
    filename=None,
    content_type="application/json",
    id_ns="json-metadata-in",
):
    mdb = get_mongo_db()
    drs_id = local_part(generate_one_id(mdb, ns=id_ns, shoulder="gfs0"))
    filename = filename or drs_id
    PortableFilename(filename)  # validates
    DrsId(drs_id)  # validates

    mdb_fs = GridFS(mdb)
    mdb_fs.put(
        content,
        _id=drs_id,
        filename=filename,
        content_type=content_type,
        encoding="utf-8",
    )
    with tempfile.TemporaryDirectory() as save_dir:
        filepath = str(Path(save_dir).joinpath(filename))
        with open(filepath, "w") as f:
            f.write(content)
        object_in = DrsObjectIn(
            **drs_metadata_for(
                filepath,
                base={
                    "description": f"metadata submitted by {username}",
                    "access_methods": [{"access_id": drs_id}],
                },
            )
        )
    self_uri = f"drs://{HOSTNAME_EXTERNAL}/{drs_id}"
    return _create_object(
        mdb, object_in, mgr_site="nmdc-runtime", drs_id=drs_id, self_uri=self_uri
    )


def _create_object(
    mdb: MongoDatabase, object_in: DrsObjectIn, mgr_site, drs_id, self_uri
):
    drs_obj = DrsObject(
        **object_in.dict(exclude_unset=True), id=drs_id, self_uri=self_uri
    )
    doc = drs_obj.dict(exclude_unset=True)
    doc["_mgr_site"] = mgr_site  # manager site
    try:
        mdb.objects.insert_one(doc)
    except DuplicateKeyError as e:
        if e.details["keyPattern"] == {"checksums.type": 1, "checksums.checksum": 1}:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="provided checksum matches existing object",
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="duplicate key error",
            )
    return doc


def _claim_job(job_id: str, mdb: MongoDatabase, site: Site):
    job_doc = raise404_if_none(mdb.jobs.find_one({"id": job_id}))
    job = Job(**job_doc)
    # check that site satisfies the job's workflow's required capabilities.
    capabilities_required = job.workflow.capability_ids or []
    for cid in capabilities_required:
        if cid not in site.capability_ids:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"client site does not have capability {cid} required to claim job",
            )

    # For now, allow site to claim same job multiple times,
    # to re-submit results given same job input config.
    job_op_for_site = mdb.operations.find_one(
        {"metadata.job.id": job.id, "metadata.site_id": site.id}
    )
    if job_op_for_site is not None:
        # raise HTTPException(
        #     status_code=status.HTTP_409_CONFLICT,
        #     detail={
        #         "msg": (
        #             f"client site already claimed job -- "
        #             f"see operation {job_op_for_site['id']}"
        #         ),
        #         "id": job_op_for_site["id"],
        #     },
        # )
        pass

    op_id = generate_one_id(mdb, "op")
    job.claims = (job.claims or []) + [JobClaim(op_id=op_id, site_id=site.id)]
    op = Operation[ResultT, JobOperationMetadata](
        **{
            "id": op_id,
            "expire_time": expiry_dt_from_now(days=30),
            "metadata": {
                "job": Job(
                    **{
                        "id": job.id,
                        "workflow": {"id": job.workflow.id},
                        "config": job.config,
                    }
                ).dict(exclude_unset=True),
                "site_id": site.id,
                "model": dotted_path_for(JobOperationMetadata),
            },
        }
    )
    mdb.operations.insert_one(op.dict())
    mdb.jobs.replace_one({"id": job.id}, job.dict(exclude_unset=True))
    return op.dict(exclude_unset=True)


def _request_dagster_run(
    job_name: str,
    run_config_data: dict,
    repository_location_name=None,
    repository_name=None,
):
    """
    Example 1:
    - job_name: hello_job
    - run_config_data: {"ops": {"hello": {"config": {"name": "Donny"}}}}

    Example 2:
    - job_name: hello_job
    - run_config_data: {}
    """
    dagster_client = get_dagster_graphql_client()
    try:
        run_id: str = dagster_client.submit_job_execution(
            job_name,
            repository_location_name=repository_location_name,
            repository_name=repository_name,
            run_config=run_config_data,
        )
        return {"type": "success", "detail": {"run_id": run_id}}
    except DagsterGraphQLClientError as exc:
        return {"type": "error", "detail": str(exc)}


def _get_dagster_run_status(run_id: str):
    dagster_client = get_dagster_graphql_client()
    try:
        run_status: PipelineRunStatus = dagster_client.get_run_status(run_id)
        return {"type": "success", "detail": str(run_status.value)}
    except DagsterGraphQLClientError as exc:
        return {"type": "error", "detail": str(exc)}
