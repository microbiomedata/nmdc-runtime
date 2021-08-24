import json
import mimetypes
import os
import subprocess
import tempfile
from datetime import datetime, timezone
from io import BytesIO
from zipfile import ZipFile

from bson import json_util
from dagster import (
    List,
    String,
    op,
    Out,
    AssetMaterialization,
    AssetKey,
    EventMetadata,
    Output,
    Failure,
    RetryPolicy,
)
from pymongo.database import Database as MongoDatabase
from starlette import status
from terminusdb_client.woqlquery import WOQLQuery as WQ
from toolz import get_in

from nmdc_runtime.api.core.idgen import generate_one_id
from nmdc_runtime.api.core.util import dotted_path_for, now, json_clean
from nmdc_runtime.api.models.job import JobOperationMetadata, JobBase, Job
from nmdc_runtime.api.models.operation import Operation, ObjectPutMetadata
from nmdc_runtime.api.models.util import ResultT
from nmdc_runtime.site.resources import RuntimeApiSiteClient
from nmdc_runtime.site.util import run_and_log
from nmdc_runtime.util import put_object, drs_object_in_for


@op
def hello(context):
    """
    A solid definition. This example solid outputs a single string.

    For more hints about writing Dagster solids, see our documentation overview on Solids:
    https://docs.dagster.io/overview/solids-pipelines/solids
    """
    out = "Hello, NMDC!"
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
        raise Failure(f"create_object_from_op failed")
    obj = rv.json()
    context.log.info(f'Created /objects/{obj["id"]}')
    mdb = context.resources.mongo.db
    rv = mdb.operations.delete_one({"id": op["id"]})
    if rv.deleted_count != 1:
        context.log.error("deleting op failed")
    yield AssetMaterialization(
        asset_key=AssetKey(["object", obj["name"]]),
        description="output of metadata-translation run_etl",
        metadata={"object_id": EventMetadata.text(obj["id"])},
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
        f"cd /opt/dagster/lib/metadata-translation/ && make build-merged-db", context
    )
    storage_path = (
        "/opt/dagster/lib/metadata-translation/src/data/nmdc_merged_data.tsv.zip"
    )
    yield AssetMaterialization(
        asset_key=AssetKey(["gold_translation", "merged_data.tsv.zip"]),
        description="input to metadata-translation run_etl",
        metadata={
            "path": EventMetadata.path(storage_path),
        },
    )
    yield Output(storage_path, "merged_data_path")


@op(
    required_resource_keys={"runtime_api_site_client"},
)
def run_etl(context, merged_data_path: str):
    context.log.info("metadata-translation: running `make run-etl`")
    if not os.path.exists(merged_data_path):
        raise Failure(description=f"merged_db not present at {merged_data_path}")
    run_and_log(f"cd /opt/dagster/lib/metadata-translation/ && make run-etl", context)
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
            "path": EventMetadata.path(storage_path),
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
def construct_job(context) -> Job:
    mdb: MongoDatabase = context.resources.mongo.db
    job = JobBase(**context.solid_config["job_base"])
    object_id_latest = context.solid_config["object_id_latest"]
    doc = {
        "id": generate_one_id(mdb, "jobs"),
        "workflow": {"id": job.workflow.id},
        "config": {"object_id_latest": object_id_latest},
        "created_at": now(),
    }
    return Job(**doc)


@op(required_resource_keys={"mongo"})
def maybe_post_job(context, job: Job):
    mdb: MongoDatabase = context.resources.mongo.db
    job_docs = list(mdb.jobs.find({"workflow.id": job.workflow.id}))
    prev_object_ids = [get_in(["config", "object_id_latest"], d) for d in job_docs]
    job_object_id = job.config.get("object_id_latest")
    if job_object_id in prev_object_ids:
        context.log.info(
            f"{job.workflow.id} job for object id {job_object_id} already posted"
        )
        yield Output(None)
        return

    object_id_timestamps = {
        d["id"]: d["created_time"]
        for d in mdb.objects.find(
            {"id": {"$in": prev_object_ids + [job_object_id]}}, ["id", "created_time"]
        )
    }
    candidate_job_object_id_timestamp = object_id_timestamps[job_object_id]
    for id_, ts in object_id_timestamps.items():
        if ts > candidate_job_object_id_timestamp:
            context.log.info(
                f"{job.workflow.id} job already posted for object id {id_} "
                f"created later than {job_object_id}"
            )
            yield Output(None)
            return

    mdb.jobs.insert_one(json_clean(job, model=Job, exclude_unset=True))
    yield AssetMaterialization(
        asset_key=AssetKey(["job", job.workflow.id]),
        description=f"workflow job",
        metadata={
            "object_id_latest": EventMetadata.text(job_object_id),
        },
    )
    yield Output(job)


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
    # TODO which of other_job_docs are unclaimed? (no operations)? Delete them.
