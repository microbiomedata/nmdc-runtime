import json
import mimetypes
import os
import subprocess
import tempfile
from collections import defaultdict
from datetime import datetime, timezone
from io import BytesIO
from typing import Dict
from zipfile import ZipFile

import fastjsonschema
from bson import ObjectId, json_util
from dagster import (
    Any,
    AssetKey,
    AssetMaterialization,
    Failure,
    List,
    MetadataValue,
    OpExecutionContext,
    Out,
    Output,
    RetryPolicy,
    String,
    op,
)
from fastjsonschema import JsonSchemaValueException
from gridfs import GridFS
from nmdc_runtime.api.core.idgen import generate_one_id
from nmdc_runtime.api.core.metadata import (
    _validate_changesheet,
    df_from_sheet_in,
    get_collection_for_id,
    map_id_to_collection,
)
from nmdc_runtime.api.core.util import dotted_path_for, json_clean, now
from nmdc_runtime.api.models.job import Job, JobOperationMetadata
from nmdc_runtime.api.models.metadata import ChangesheetIn
from nmdc_runtime.api.models.operation import (
    ObjectPutMetadata,
    Operation,
    UpdateOperationRequest,
)
from nmdc_runtime.api.models.run import _add_run_complete_event
from nmdc_runtime.api.models.util import ResultT
from nmdc_runtime.site.drsobjects.ingest import mongo_add_docs_result_as_dict
from nmdc_runtime.site.drsobjects.registration import specialize_activity_set_docs
from nmdc_runtime.site.resources import GoldApiClient, RuntimeApiSiteClient
from nmdc_runtime.site.util import collection_indexed_on_id, run_and_log
from nmdc_runtime.util import drs_object_in_for, pluralize, put_object
from nmdc_runtime.util import get_nmdc_jsonschema_dict
from pydantic import BaseModel
from pymongo.database import Database as MongoDatabase
from starlette import status
from terminusdb_client.woqlquery import WOQLQuery as WQ
from toolz import assoc, dissoc, get_in


@op
def hello(context):
    """
    A solid definition. This example solid outputs a single string.

    For more hints about writing Dagster solids, see our documentation overview on Solids:
    https://docs.dagster.io/overview/solids-pipelines/solids
    """
    name = context.op_config.get("name", "NMDC") if context.op_config else "NMDC"
    out = f"Hello, {name}!"
    context.log.info(out)
    return out


@op
def log_env(context):
    env = subprocess.check_output("printenv", shell=True).decode()
    out = [line for line in env.splitlines() if line.startswith("DAGSTER_")]
    context.log.info("\n".join(out))


@op(required_resource_keys={"terminus"})
def list_databases(context) -> List[String]:
    client = context.resources.terminus.client
    list_ = client.list_databases()
    context.log.info(f"databases: {list_}")
    return list_


@op(required_resource_keys={"mongo"})
def mongo_stats(context) -> List[str]:
    db = context.resources.mongo.db
    collection_names = db.list_collection_names()
    context.log.info(str(collection_names))
    return collection_names


@op(required_resource_keys={"terminus"})
def update_schema(context):
    with tempfile.TemporaryDirectory() as tmpdirname:
        try:
            context.log.info("shallow-cloning nmdc-schema repo")
            subprocess.check_output(
                "git clone https://github.com/microbiomedata/nmdc-schema.git"
                f" --branch main --single-branch {tmpdirname}/nmdc-schema",
                shell=True,
            )
            context.log.info("generating TerminusDB JSON-LD from NMDC LinkML")
            subprocess.check_output(
                f"gen-terminusdb {tmpdirname}/nmdc-schema/src/schema/nmdc.yaml"
                f" > {tmpdirname}/nmdc.terminus.json",
                shell=True,
            )
        except subprocess.CalledProcessError as e:
            if e.stdout:
                context.log.debug(e.stdout.decode())
            if e.stderr:
                context.log.error(e.stderr.decode())
            context.log.debug(str(e.returncode))
            raise e

        with open(f"{tmpdirname}/nmdc.terminus.json") as f:
            woql_dict = json.load(f)

    context.log.info("Updating terminus schema via WOQLQuery")
    rv = WQ(query=woql_dict).execute(
        context.resources.terminus.client, "update schema via WOQL"
    )
    context.log.info(str(rv))
    return rv


@op(
    required_resource_keys={"mongo", "runtime_api_site_client"},
    retry_policy=RetryPolicy(max_retries=2),
)
def local_file_to_api_object(context, file_info):
    client: RuntimeApiSiteClient = context.resources.runtime_api_site_client
    storage_path: str = file_info["storage_path"]
    mime_type = file_info.get("mime_type")
    if mime_type is None:
        mime_type = mimetypes.guess_type(storage_path)[0]
    rv = client.put_object_in_site(
        {"mime_type": mime_type, "name": storage_path.rpartition("/")[-1]}
    )
    if not rv.status_code == status.HTTP_200_OK:
        raise Failure(description=f"put_object_in_site failed: {rv.content}")
    op = rv.json()
    context.log.info(f"put_object_in_site: {op}")
    rv = put_object(storage_path, op["metadata"]["url"])
    if not rv.status_code == status.HTTP_200_OK:
        raise Failure(description=f"put_object failed: {rv.content}")
    op_patch = {"done": True, "result": drs_object_in_for(storage_path, op)}
    rv = client.update_operation(op["id"], op_patch)
    if not rv.status_code == status.HTTP_200_OK:
        raise Failure(description="update_operation failed")
    op = rv.json()
    context.log.info(f"update_operation: {op}")
    rv = client.create_object_from_op(op)
    if rv.status_code != status.HTTP_201_CREATED:
        raise Failure("create_object_from_op failed")
    obj = rv.json()
    context.log.info(f'Created /objects/{obj["id"]}')
    mdb = context.resources.mongo.db
    rv = mdb.operations.delete_one({"id": op["id"]})
    if rv.deleted_count != 1:
        context.log.error("deleting op failed")
    yield AssetMaterialization(
        asset_key=AssetKey(["object", obj["name"]]),
        description="output of metadata-translation run_etl",
        metadata={"object_id": MetadataValue.text(obj["id"])},
    )
    yield Output(obj)


@op(
    out={
        "merged_data_path": Out(
            str,
            description="path to TSV merging of source metadata",
        )
    }
)
def build_merged_db(context) -> str:
    context.log.info("metadata-translation: running `make build-merged-db`")
    run_and_log(
        "cd /opt/dagster/lib/metadata-translation/ && make build-merged-db", context
    )
    storage_path = (
        "/opt/dagster/lib/metadata-translation/src/data/nmdc_merged_data.tsv.zip"
    )
    yield AssetMaterialization(
        asset_key=AssetKey(["gold_translation", "merged_data.tsv.zip"]),
        description="input to metadata-translation run_etl",
        metadata={"path": MetadataValue.path(storage_path)},
    )
    yield Output(storage_path, "merged_data_path")


@op(
    required_resource_keys={"runtime_api_site_client"},
)
def run_etl(context, merged_data_path: str):
    context.log.info("metadata-translation: running `make run-etl`")
    if not os.path.exists(merged_data_path):
        raise Failure(description=f"merged_db not present at {merged_data_path}")
    run_and_log("cd /opt/dagster/lib/metadata-translation/ && make run-etl", context)
    storage_path = (
        "/opt/dagster/lib/metadata-translation/src/data/nmdc_database.json.zip"
    )
    with ZipFile(storage_path) as zf:
        name = zf.namelist()[0]
        with zf.open(name) as f:
            rv = json.load(f)
    context.log.info(f"nmdc_database.json keys: {list(rv.keys())}")
    yield AssetMaterialization(
        asset_key=AssetKey(["gold_translation", "database.json.zip"]),
        description="output of metadata-translation run_etl",
        metadata={
            "path": MetadataValue.path(storage_path),
        },
    )
    yield Output({"storage_path": storage_path})


@op(required_resource_keys={"mongo"})
def get_operation(context):
    mdb = context.resources.mongo.db
    id_op = context.solid_config.get("operation_id")
    doc = mdb.operations.find_one({"id": id_op})
    if doc is None:
        raise Failure(description=f"operation {id_op} not found")
    context.log.info(f"got operation {id_op}")
    return Operation(**doc)


@op(
    required_resource_keys={"runtime_api_site_client", "mongo"},
    retry_policy=RetryPolicy(max_retries=2),
)
def produce_curated_db(context, op: Operation):
    client: RuntimeApiSiteClient = context.resources.runtime_api_site_client
    mdb: MongoDatabase = context.resources.mongo.db
    op = Operation[ResultT, JobOperationMetadata](**op.dict())
    op_meta: JobOperationMetadata = op.metadata
    job_id = op_meta.job.id
    job = mdb.jobs.find_one({"id": job_id})
    o_id = get_in(["config", "object_id_latest"], job)
    rv = client.get_object_bytes(o_id)

    with ZipFile(BytesIO(rv.content)) as myzip:
        name = next(n for n in myzip.namelist() if n.endswith("nmdc_database.json"))
        with myzip.open(name) as f:
            nmdc_database = json.load(f)

    context.log.info(f"{list(nmdc_database.keys())}")
    # TODO do the curation. :)
    return nmdc_database


@op(required_resource_keys={"runtime_api_site_client"})
def create_objects_from_ops(context, op_docs: list):
    client: RuntimeApiSiteClient = context.resources.runtime_api_site_client
    responses = [client.create_object_from_op(doc) for doc in op_docs]
    if {r.status_code for r in responses} == {201}:
        context.log.info("All OK")
    elif responses:
        raise Failure(f"Unexpected response(s): {[r.text for r in responses]}")
    return op_docs


@op
def filter_ops_done_object_puts() -> str:
    return json_util.dumps(
        {
            "done": True,
            "metadata.model": dotted_path_for(ObjectPutMetadata),
        }
    )


@op
def filter_ops_undone_expired() -> str:
    return json_util.dumps(
        {
            "done": {"$ne": True},
            "expire_time": {"$lt": datetime.now(timezone.utc)},
        }
    )


@op(required_resource_keys={"runtime_api_site_client"})
def list_operations(context, filter_: str) -> list:
    client = context.resources.runtime_api_site_client
    ops = [op.dict() for op in client.list_operations({"filter": filter_})]
    context.log.info(str(len(ops)))
    return ops


@op(required_resource_keys={"mongo"})
def delete_operations(context, op_docs: list):
    mdb = context.resources.mongo.db
    rv = mdb.operations.delete_many({"id": {"$in": [doc["id"] for doc in op_docs]}})
    context.log.info(f"Deleted {rv.deleted_count} of {len(op_docs)}")
    if rv.deleted_count != len(op_docs):
        context.log.error("Didn't delete all.")


@op(required_resource_keys={"mongo"})
def construct_jobs(context) -> List[Job]:
    mdb: MongoDatabase = context.resources.mongo.db
    docs = [
        dict(**base, id=generate_one_id(mdb, "jobs"), created_at=now())
        for base in context.solid_config["base_jobs"]
    ]
    return [Job(**d) for d in docs]


@op(required_resource_keys={"mongo"})
def maybe_post_jobs(context, jobs: List[Job]):
    mdb: MongoDatabase = context.resources.mongo.db
    n_posted = 0
    for job in jobs:
        job_docs = list(mdb.jobs.find({"workflow.id": job.workflow.id}))
        posted_job_object_ids = [get_in(["config", "object_id"], d) for d in job_docs]
        job_object_id = job.config.get("object_id")
        if job_object_id in posted_job_object_ids:
            context.log.info(
                f"{job.workflow.id} job for object id {job_object_id} already posted"
            )
            continue

        object_id_timestamps = {
            d["id"]: d["created_time"]
            for d in mdb.objects.find(
                {"id": {"$in": posted_job_object_ids + [job_object_id]}},
                ["id", "created_time"],
            )
        }
        candidate_job_object_id_timestamp = object_id_timestamps[job_object_id]
        for id_, ts in object_id_timestamps.items():
            if ts > candidate_job_object_id_timestamp:
                context.log.info(
                    f"{job.workflow.id} job already posted for object id {id_} "
                    f"created later than {job_object_id}"
                )
                break

        mdb.jobs.insert_one(json_clean(job, model=Job, exclude_unset=True))
        yield AssetMaterialization(
            asset_key=AssetKey(["job", job.workflow.id]),
            description="workflow job",
            metadata={
                "object_id": MetadataValue.text(job_object_id),
            },
        )
        n_posted += 1
    context.log.info(f'{n_posted} {pluralize("job", n_posted)}')
    yield Output(n_posted)


@op(required_resource_keys={"mongo"})
def remove_unclaimed_obsolete_jobs(context, job: Job):
    mdb: MongoDatabase = context.resources.mongo.db
    job_object_id = job.config.get("object_id_latest")
    other_job_docs = list(
        mdb.jobs.find(
            {
                "workflow.id": job.workflow.id,
                "config.object_id_latest": {"$ne": job_object_id},
            }
        )
    )
    print(other_job_docs)
    # TODO which of other_job_docs are unclaimed? (no operations)? Delete them.


@op(required_resource_keys={"mongo"})
def get_changesheet_in(context) -> ChangesheetIn:
    mdb: MongoDatabase = context.resources.mongo.db
    object_id = context.solid_config.get("object_id")
    mdb_fs = GridFS(mdb)
    grid_out = mdb_fs.get(object_id)
    return ChangesheetIn(
        name=grid_out.filename, content_type=grid_out.content_type, text=grid_out.read()
    )


@op(required_resource_keys={"mongo"})
def perform_changesheet_updates(context, sheet_in: ChangesheetIn):
    mdb: MongoDatabase = context.resources.mongo.db
    op_id = context.solid_config.get("operation_id")
    try:
        df_change = df_from_sheet_in(sheet_in, mdb)
        validation_result = _validate_changesheet(df_change, mdb)
    except Exception as e:
        raise Failure(str(e))

    update_cmd = validation_result["update_cmd"]
    results_of_updates = validation_result["results_of_updates"]

    id_dict = map_id_to_collection(mdb)
    docs_to_upsert = defaultdict(list)
    for r in results_of_updates:
        collection_name = get_collection_for_id(r["id"], id_dict)
        docs_to_upsert[collection_name].append(r["doc_after"])
    context.resources.mongo.add_docs(docs_to_upsert)
    op = Operation(**mdb.operations.find_one({"id": op_id}))
    op.done = True
    op.result = {"update_cmd": json.dumps(update_cmd)}
    op_doc = op.dict(exclude_unset=True)
    mdb.operations.replace_one({"id": op_id}, op_doc)
    return ["/operations/" + op_doc["id"]]


@op(required_resource_keys={"runtime_api_site_client"})
def get_json_in(context):
    object_id = context.solid_config.get("object_id")
    client: RuntimeApiSiteClient = context.resources.runtime_api_site_client
    rv = client.get_object_bytes(object_id)
    if rv.status_code != 200:
        raise Failure(
            description=f"error code {rv.status_code} for {rv.request.url}: {rv.text}"
        )
    return rv.json()


def ensure_data_object_type(docs: Dict[str, list], mdb: MongoDatabase):
    """Does not ensure ordering of `docs`."""

    if ("data_object_set" not in docs) or len(docs["data_object_set"]) == 0:
        return docs, 0

    do_docs = docs["data_object_set"]

    class FileTypeEnumBase(BaseModel):
        name: str
        description: str
        filter: str  # JSON-encoded data_object_set mongo collection filter document

    class FileTypeEnum(FileTypeEnumBase):
        id: str

    temp_collection_name = f"tmp.data_object_set.{ObjectId()}"
    temp_collection = mdb[temp_collection_name]
    temp_collection.insert_many(do_docs)
    temp_collection.create_index("id")

    def fte_matches(fte_filter: str):
        return [
            dissoc(d, "_id") for d in mdb.temp_collection.find(json.loads(fte_filter))
        ]

    do_docs_map = {d["id"]: d for d in do_docs}

    n_docs_with_types_added = 0

    for fte_doc in mdb.file_type_enum.find():
        fte = FileTypeEnum(**fte_doc)
        docs_matching = fte_matches(fte.filter)
        for doc in docs_matching:
            if "data_object_type" not in doc:
                do_docs_map[doc["id"]] = assoc(doc, "data_object_type", fte.id)
                n_docs_with_types_added += 1

    mdb.drop_collection(temp_collection_name)
    return (
        assoc(
            docs, "data_object_set", [dissoc(v, "_id") for v in do_docs_map.values()]
        ),
        n_docs_with_types_added,
    )


@op(required_resource_keys={"runtime_api_site_client", "mongo"})
def perform_mongo_updates(context, json_in):
    mongo = context.resources.mongo
    client: RuntimeApiSiteClient = context.resources.runtime_api_site_client
    op_id = context.solid_config.get("operation_id")

    docs = json_in
    docs, _ = specialize_activity_set_docs(docs)
    docs, n_docs_with_types_added = ensure_data_object_type(docs, mongo.db)
    context.log.info(f"added `data_object_type` to {n_docs_with_types_added} docs")
    context.log.debug(f"{docs}")

    nmdc_jsonschema = get_nmdc_jsonschema_dict()

    # commenting out the change to the schema dict with IDs from Mongo collection
    # nmdc_jsonschema["$defs"]["FileTypeEnum"]["enum"] = mongo.db.file_type_enum.distinct(
    #    "id"
    # )
    nmdc_jsonschema_validate = fastjsonschema.compile(nmdc_jsonschema)

    try:
        _ = nmdc_jsonschema_validate(docs)
    except JsonSchemaValueException as e:
        raise Failure(str(e))
    coll_has_id_index = collection_indexed_on_id(mongo.db)
    if all(coll_has_id_index[coll] for coll in docs.keys()):
        replace = True
    elif all(not coll_has_id_index[coll] for coll in docs.keys()):
        replace = False  # wasting time trying to upsert by `id`.
    else:
        raise Failure(
            "Simultaneous addition of non-`id`ed collections and `id`-ed collections"
            " is not supported at this time."
        )
    op_result = mongo.add_docs(docs, validate=False, replace=replace)
    op_patch = UpdateOperationRequest(
        done=True,
        result=mongo_add_docs_result_as_dict(op_result),
        metadata={"done_at": datetime.now(timezone.utc).isoformat(timespec="seconds")},
    )
    op_doc = client.update_operation(op_id, op_patch).json()
    return ["/operations/" + op_doc["id"]]


@op(required_resource_keys={"mongo"})
def add_output_run_event(context: OpExecutionContext, outputs: List[str]):
    mdb = context.resources.mongo.db
    run_event_doc = mdb.run_events.find_one(
        {"run.facets.nmdcRuntime_dagsterRunId": context.run_id}
    )
    if run_event_doc:
        nmdc_run_id = run_event_doc["run"]["id"]
        return _add_run_complete_event(run_id=nmdc_run_id, mdb=mdb, outputs=outputs)
    else:
        context.log.info(f"No NMDC RunEvent doc for Dagster Run {context.run_id}")


@op(required_resource_keys={"gold_api_client"}, config_schema={"study_id": str})
def gold_biosamples_by_study(context: OpExecutionContext):
    client: GoldApiClient = context.resources.gold_api_client
    return client.fetch_biosamples_by_study(context.op_config["study_id"])


@op(required_resource_keys={"gold_api_client"}, config_schema={"study_id": str})
def gold_projects_by_study(context: OpExecutionContext):
    client: GoldApiClient = context.resources.gold_api_client
    return client.fetch_projects_by_study(context.op_config["study_id"])

@op
def gold_biosample_ids(context, docs: List[Dict[str, Any]]):
    return unique_field_values(docs, "biosampleGoldId")

def unique_field_values(docs: List[Dict[str, Any]], field: str):
    return {doc[field] for doc in docs if field in doc}
