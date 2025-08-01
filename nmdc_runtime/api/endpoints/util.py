import logging
import os
import tempfile
from datetime import datetime
from functools import lru_cache
from json import JSONDecodeError
from pathlib import Path
from time import time_ns
from typing import List, Optional, Set, Tuple
from zoneinfo import ZoneInfo

from bson import json_util
from dagster import DagsterRunStatus
from dagster_graphql import DagsterGraphQLClientError
from fastapi import HTTPException
from gridfs import GridFS
from nmdc_runtime.api.core.idgen import generate_one_id, local_part
from nmdc_runtime.api.core.util import (
    dotted_path_for,
    expiry_dt_from_now,
    raise404_if_none,
)
from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.models.job import Job, JobClaim, JobOperationMetadata
from nmdc_runtime.api.models.object import (
    DrsId,
    DrsObject,
    DrsObjectIn,
    PortableFilename,
)
from nmdc_runtime.api.models.operation import Operation
from nmdc_runtime.api.models.run import (
    RunUserSpec,
    _add_run_fail_event,
    _add_run_requested_event,
    _add_run_started_event,
    get_dagster_graphql_client,
)
from nmdc_runtime.api.models.site import Site
from nmdc_runtime.api.models.user import User
from nmdc_runtime.api.models.util import (
    FindRequest,
    ListRequest,
    ResultT,
)
from nmdc_runtime.util import drs_metadata_for
from pymongo.collection import Collection as MongoCollection
from pymongo.database import Database as MongoDatabase
from pymongo.errors import DuplicateKeyError
from starlette import status
from toolz import assoc_in, concat, dissoc, get_in, merge

BASE_URL_INTERNAL = os.getenv("API_HOST")
BASE_URL_EXTERNAL = os.getenv("API_HOST_EXTERNAL")
HOSTNAME_EXTERNAL = BASE_URL_EXTERNAL.split("://", 1)[-1]


def check_filter(filter_: str):
    """A pass-through function that checks if `filter_` is parsable as a JSON object. Raises otherwise."""
    filter_ = filter_.strip()
    if not filter_.startswith("{") or not filter_.endswith("}"):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"The given `filter` is not a valid JSON object, which must start with '{{' and end with '}}'.",
        )
    try:
        json_util.loads(filter_)
    except JSONDecodeError as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Given `filter` is not valid JSON: {e}",
        )
    return filter_


def list_resources(req: ListRequest, mdb: MongoDatabase, collection_name: str):
    r"""
    Returns a dictionary containing the requested MongoDB documents, maybe alongside pagination information.

    Note: If the specified `ListRequest` has a non-zero `max_page_size` number and the number of documents matching the
          filter criteria is _larger_ than that number, this function will paginate the resources. Paginating the
          resources currently involves MongoDB sorting _all_ matching documents, which can take a long time, especially
          when the collection involved contains many documents.
    """

    id_field = "id"
    if "id_1" not in mdb[collection_name].index_information():
        logging.warning(
            f"list_resources: no index set on 'id' for collection {collection_name}"
        )
        id_field = (
            "_id"  # currently expected for `functional_annotation_agg` collection
        )
    limit = req.max_page_size
    filter_ = json_util.loads(check_filter(req.filter)) if req.filter else {}
    projection = (
        list(set(comma_separated_values(req.projection)) | {id_field})
        if req.projection
        else None
    )
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
        if id_field in filter_:
            filter_[id_field] = merge(filter_[id_field], {"$gt": last_id})
        else:
            filter_ = merge(filter_, {id_field: {"$gt": last_id}})

    # If limit is 0, the response will include all results (bypassing pagination altogether).
    if (limit == 0) or (mdb[collection_name].count_documents(filter=filter_) <= limit):
        rv = {
            "resources": list(
                mdb[collection_name].find(filter=filter_, projection=projection)
            )
        }
        return rv
    else:
        resources = list(
            mdb[collection_name].find(
                filter=filter_,
                projection=projection,
                limit=limit,
                sort=[(id_field, 1)],
                allow_disk_use=True,
            )
        )
        last_id = resources[-1][id_field]
        token = generate_one_id(mdb, "page_tokens")
        # TODO unify with `/queries:run` query continuation model
        #  => {_id: cursor/token, query: <full query>, last_id: <>, last_modified: <>}
        mdb.page_tokens.insert_one(
            {"_id": token, "ns": collection_name, "last_id": last_id}
        )
        return {"resources": resources, "next_page_token": token}


def coerce_to_float_if_possible(val):
    r"""
    Converts the specified value into a floating-point number if possible;
    raising a `ValueError` if not possible.
    """
    try:
        return float(val)
    except ValueError:
        return val


def comma_separated_values(s: str):
    r"""
    Returns a list of the comma-delimited substrings of the specified string. Discards any whitespace
    surrounding each substring.

    Reference: https://docs.python.org/3/library/re.html#re.split

    >>> comma_separated_values("apple, banana, cherry")
    ['apple', 'banana', 'cherry']
    """
    return [v.strip() for v in s.split(",")]


def get_mongo_filter(filter_str):
    r"""
    Convert a str in the domain-specific language (DSL) solicited by `nmdc_runtime.api.models.util.FindRequest.filter`
    -- i.e., a comma-separated list of `attribute:value` pairs, where the `value` can include a comparison operator
    (e.g. `>=`) and where if the attribute is of type _string_ and has the suffix `.search` appended to its name
    then the server should perform a full-text search
    -- to a corresponding MongoDB filter representation for e.g. passing to a collection `find` call.
    """
    filter_ = {}
    if not filter_str:
        return filter_

    pairs = comma_separated_values(filter_str)
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
                    filter_[attr] = {key: coerce_to_float_if_possible(spec[len(op) :])}
                    break
            else:
                filter_[attr] = spec
    return filter_


def get_mongo_sort(sort_str) -> Optional[List[Tuple[str, int]]]:
    """
    Parse `sort_str` and a str of the form "attribute:spec[,attribute:spec]*",
    where spec is `asc` (ascending -- the default if no spec) or `desc` (descending),
    and return a value suitable to pass as a `sort` kwarg to a mongo collection `find` call.
    """
    sort_ = []
    if not sort_str:
        return None

    pairs = comma_separated_values(sort_str)
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


def strip_oid(doc: dict) -> dict:
    r"""
    Returns a copy of the specified dictionary, that has no `_id` key.
    """
    return dissoc(doc, "_id")


def timeit(cursor):
    """Collect from cursor and return time taken in milliseconds."""
    tic = time_ns()
    results = list(cursor)
    toc = time_ns()
    return results, int(round((toc - tic) / 1e6))


def find_resources(req: FindRequest, mdb: MongoDatabase, collection_name: str):
    """Find nmdc schema collection entities that match the FindRequest.

    "resources" is used generically here, as in "Web resources", e.g. Uniform Resource Identifiers (URIs).
    """
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
    projection = (
        list(set(comma_separated_values(req.fields)) | {"id"}) if req.fields else None
    )
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
                filter=filter_,
                skip=skip,
                limit=limit,
                sort=sort_,
                projection=projection,
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
        if req.fields:
            rv["meta"]["fields"] = req.fields

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
            mdb[collection_name].find(
                filter=filter_, limit=limit, sort=sort_for_cursor, projection=projection
            )
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
        if req.fields:
            rv["meta"]["fields"] = req.fields
    return rv


def find_resources_spanning(
    req: FindRequest, mdb: MongoDatabase, collection_names: Set[str]
):
    """Find nmdc schema collection entities -- here, across multiple collections -- that match the FindRequest.

    This is useful for collections that house documents that are subclasses of a common ancestor class.

    "resources" is used generically here, as in "Web resources", e.g. Uniform Resource Identifiers (URIs).
    """
    if req.cursor or not req.page:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="This resource only supports page-based pagination",
        )

    if len(collection_names) == 0:
        return {
            "meta": {
                "mongo_filter_dict": get_mongo_filter(req.filter),
                "count": 0,
                "db_response_time_ms": 0,
                "page": req.page,
                "per_page": req.per_page,
            },
            "results": [],
            "group_by": [],
        }

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
    r"""
    Returns True if there are any documents in the collection that meet the filter requirements.
    """
    return collection.count_documents(filter_) > 0


def persist_content_and_get_drs_object(
    content: str,
    description: str,
    username="(anonymous)",
    filename=None,
    content_type="application/json",
    id_ns="json-metadata-in",
    exists_ok=False,
):
    """Persist a Data Repository Service (DRS) object.

    An object may be a blob, analogous to a file, or a bundle, analogous to a folder. Sites register objects,
    and sites must ensure that these objects are accessible to the NMDC data broker.
    An object may be associated with one or more object types, useful for triggering workflows.

    Reference: https://ga4gh.github.io/data-repository-service-schemas/preview/release/drs-1.1.0/docs/#_drs_datatypes
    """
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
        now_to_the_minute = datetime.now(tz=ZoneInfo("America/Los_Angeles")).isoformat(
            timespec="minutes"
        )
        object_in = DrsObjectIn(
            **drs_metadata_for(
                filepath,
                base={
                    "description": (
                        description
                        + f" (created by/for {username}"
                        + f" at {now_to_the_minute})"
                    ),
                    "access_methods": [{"access_id": drs_id}],
                },
                timestamp=now_to_the_minute,
            )
        )
    self_uri = f"drs://{HOSTNAME_EXTERNAL}/{drs_id}"
    return _create_object(
        mdb,
        object_in,
        mgr_site="nmdc-runtime",
        drs_id=drs_id,
        self_uri=self_uri,
        exists_ok=exists_ok,
    )


def _create_object(
    mdb: MongoDatabase,
    object_in: DrsObjectIn,
    mgr_site,
    drs_id,
    self_uri,
    exists_ok=False,
):
    """Helper function for creating a Data Repository Service (DRS) object."""
    drs_obj = DrsObject(
        **object_in.model_dump(exclude_unset=True),
        id=drs_id,
        self_uri=self_uri,
    )
    doc = drs_obj.model_dump(exclude_unset=True)
    doc["_mgr_site"] = mgr_site  # manager site
    try:
        mdb.objects.insert_one(doc)
    except DuplicateKeyError as e:
        if e.details["keyPattern"] == {"checksums.type": 1, "checksums.checksum": 1}:
            if exists_ok:
                return mdb.objects.find_one(
                    {
                        "checksums": {
                            "$elemMatch": {
                                "type": e.details["keyValue"]["checksums.type"],
                                "checksum": e.details["keyValue"]["checksums.checksum"],
                            }
                        }
                    }
                )
            else:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"provided checksum matches existing object: {e.details['keyValue']}",
                )
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="duplicate key error",
            )
    return doc


def _claim_job(job_id: str, mdb: MongoDatabase, site: Site):
    r"""
    TODO: Document this function.
    """
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
                        "workflow": job.workflow,
                        "config": job.config,
                    }
                ).model_dump(exclude_unset=True),
                "site_id": site.id,
                "model": dotted_path_for(JobOperationMetadata),
            },
        }
    )
    mdb.operations.insert_one(op.model_dump())
    mdb.jobs.replace_one({"id": job.id}, job.model_dump(exclude_unset=True))

    return op.model_dump(exclude_unset=True)


@lru_cache
def map_nmdc_workflow_id_to_dagster_job_name():
    """Returns a dictionary mapping nmdc_workflow_id to dagster_job_name."""
    return {
        "metadata-in-1.0.0": "apply_metadata_in",
        "export-study-biosamples-as-csv-1.0.0": "export_study_biosamples_metadata",
        "gold_study_to_database": "gold_study_to_database",
    }


def ensure_run_config_data(
    nmdc_workflow_id: str,
    nmdc_workflow_inputs: List[str],
    run_config_data: dict,
    mdb: MongoDatabase,
    user: User,
):
    r"""
    Ensures that run_config_data has entries for certain nmdc workflow ids.
    Returns return_config_data.
    """
    if nmdc_workflow_id == "export-study-biosamples-as-csv-1.0.0":
        run_config_data = assoc_in(
            run_config_data,
            ["ops", "get_study_biosamples_metadata", "config", "study_id"],
            nmdc_workflow_inputs[0],
        )
        run_config_data = assoc_in(
            run_config_data,
            ["ops", "get_study_biosamples_metadata", "config", "username"],
            user.username,
        )
        return run_config_data
    if nmdc_workflow_id == "gold_study_to_database":
        run_config_data = assoc_in(
            run_config_data,
            ["ops", "get_gold_study_pipeline_inputs", "config", "study_id"],
            nmdc_workflow_inputs[0],
        )
        run_config_data = assoc_in(
            run_config_data,
            ["ops", "export_json_to_drs", "config", "username"],
            user.username,
        )
        return run_config_data
    else:
        return run_config_data


def inputs_for(nmdc_workflow_id, run_config_data):
    """Returns a URI path for given nmdc_workflow_id, constructed from run_config_data."""
    if nmdc_workflow_id == "metadata-in-1.0.0":
        return [
            "/objects/"
            + get_in(["ops", "get_json_in", "config", "object_id"], run_config_data)
        ]
    if nmdc_workflow_id == "export-study-biosamples-as-csv-1.0.0":
        return [
            "/studies/"
            + get_in(
                ["ops", "get_study_biosamples_metadata", "config", "study_id"],
                run_config_data,
            )
        ]
    if nmdc_workflow_id == "gold_study_to_database":
        return [
            "/studies/"
            + get_in(
                ["ops", "get_gold_study_pipeline_inputs", "config", "study_id"],
                run_config_data,
            )
        ]


def _request_dagster_run(
    nmdc_workflow_id: str,
    nmdc_workflow_inputs: List[str],
    extra_run_config_data: dict,
    mdb: MongoDatabase,
    user: User,
    repository_location_name=None,
    repository_name=None,
):
    r"""
    Requests a Dagster run using the specified parameters.
    Returns a json dictionary indicating the job's success or failure.
    This is a generic wrapper.
    """
    dagster_job_name = map_nmdc_workflow_id_to_dagster_job_name()[nmdc_workflow_id]

    extra_run_config_data = ensure_run_config_data(
        nmdc_workflow_id, nmdc_workflow_inputs, extra_run_config_data, mdb, user
    )

    # add REQUESTED RunEvent
    nmdc_run_id = _add_run_requested_event(
        run_spec=RunUserSpec(
            job_id=nmdc_workflow_id,
            run_config=extra_run_config_data,
            inputs=inputs_for(nmdc_workflow_id, extra_run_config_data),
        ),
        mdb=mdb,
        user=user,
    )

    dagster_client = get_dagster_graphql_client()
    try:
        dagster_run_id: str = dagster_client.submit_job_execution(
            dagster_job_name,
            repository_location_name=repository_location_name,
            repository_name=repository_name,
            run_config=extra_run_config_data,
        )

        # add STARTED RunEvent
        _add_run_started_event(run_id=nmdc_run_id, mdb=mdb)
        mdb.run_events.find_one_and_update(
            filter={"run.id": nmdc_run_id, "type": "STARTED"},
            update={"$set": {"run.facets.nmdcRuntime_dagsterRunId": dagster_run_id}},
            sort=[("time", -1)],
        )

        return {"type": "success", "detail": {"run_id": nmdc_run_id}}
    except DagsterGraphQLClientError as exc:
        # add FAIL RunEvent
        _add_run_fail_event(run_id=nmdc_run_id, mdb=mdb)

        return {
            "type": "error",
            "detail": {"run_id": nmdc_run_id, "error_detail": str(exc)},
        }


def _get_dagster_run_status(run_id: str):
    r"""
    Returns the status (either "success" or "error") of a requested Dagster run.
    """
    dagster_client = get_dagster_graphql_client()
    try:
        run_status: DagsterRunStatus = dagster_client.get_run_status(run_id)
        return {"type": "success", "detail": str(run_status.value)}
    except DagsterGraphQLClientError as exc:
        return {"type": "error", "detail": str(exc)}


def check_action_permitted(username: str, action: str):
    """Returns True if a Mongo database action is "allowed" and "not denied"."""
    db: MongoDatabase = get_mongo_db()
    filter_ = {"username": username, "action": action}
    denied = db["_runtime.api.deny"].find_one(filter_) is not None
    allowed = db["_runtime.api.allow"].find_one(filter_) is not None
    return (not denied) and allowed
