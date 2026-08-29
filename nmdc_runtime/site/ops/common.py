"""
Dagster ops.

Note: These were the ops that remained in `nmdc_runtime/site/ops.py` after other ops were extracted
      during a refactor. That file was then moved/renamed to `nmdc_runtime/site/ops/common.py`.
"""

# TODO: Organize these imports, per PEP 8 (https://peps.python.org/pep-0008/#imports).

import csv
import json
import os
import subprocess
from collections import defaultdict
from datetime import datetime, timezone
from importlib.metadata import version
from io import BytesIO

from typing import Optional
from zipfile import ZipFile
import pandas as pd
import requests
from toolz import dissoc
from dagster_slack import SlackResource

from dagster import (
    Any,
    AssetMaterialization,
    Dict,
    Failure,
    List,
    MetadataValue,
    OpExecutionContext,
    Out,
    Output,
    RetryPolicy,
    op,
)
from gridfs import GridFS
from linkml_runtime.utils.dictutils import as_simple_dict
from linkml_runtime.utils.yamlutils import YAMLRoot

from nmdc_runtime import config
from nmdc_runtime.api.db.mongo import validate_json
from nmdc_runtime.api.core.metadata import (
    _validate_changesheet,
    df_from_sheet_in,
    get_collection_for_id,
    map_id_to_collection,
)
from nmdc_runtime.api.core.util import hash_from_str
from nmdc_runtime.api.endpoints.util import persist_content_and_get_drs_object
from nmdc_runtime.api.models.job import JobOperationMetadata
from nmdc_runtime.api.models.metadata import ChangesheetIn
from nmdc_runtime.api.models.operation import (
    Operation,
    UpdateOperationRequest,
)
from nmdc_runtime.api.models.run import _add_run_complete_event
from nmdc_runtime.api.models.util import ResultT
from nmdc_runtime.site.resources import (
    RuntimeApiSiteClient,
    RuntimeApiUserClient,
    MongoDB as MongoDBResource,
)
from nmdc_runtime.site.util import (
    schema_collection_has_index_on_id,
    nmdc_study_id_to_filename,
)
from nmdc_runtime.util import specialize_activity_set_docs
from nmdc_schema import nmdc
from pymongo.database import Database as MongoDatabase
from toolz import get_in, valfilter, identity


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
def show_version_info_op(context):
    """Logs and returns the "nmdc_runtime" package version."""
    nmdc_runtime_package_version = version("nmdc_runtime")
    context.log.info(f"nmdc_runtime package version: {nmdc_runtime_package_version}")
    return nmdc_runtime_package_version


@op
def log_env(context):
    env = subprocess.check_output("printenv", shell=True).decode()
    out = [line for line in env.splitlines() if line.startswith("DAGSTER_")]
    context.log.info("\n".join(out))


@op(required_resource_keys={"mongo"})
def mongo_stats(context) -> List[str]:
    db = context.resources.mongo.db
    collection_names = db.list_collection_names()
    context.log.info(str(collection_names))
    return collection_names


# TODO: Delete this function's definition, which is not referenced by anything.
@op(
    required_resource_keys={"runtime_api_site_client", "mongo"},
    retry_policy=RetryPolicy(max_retries=2),
)
def produce_curated_db(context, op: Operation):
    client: RuntimeApiSiteClient = context.resources.runtime_api_site_client
    mdb: MongoDatabase = context.resources.mongo.db
    op = Operation[ResultT, JobOperationMetadata](**op.model_dump())
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


@op(required_resource_keys={"runtime_api_user_client"})
def validate_metadata(context: OpExecutionContext, database: nmdc.Database):
    client: RuntimeApiUserClient = context.resources.runtime_api_user_client
    response = client.validate_metadata(database)
    body = response.json()
    if body["result"] != "All Okay!":
        raise Failure(
            description="Metadata did not validate",
            metadata={"detail": body["detail"]},
        )
    return body


@op(required_resource_keys={"runtime_api_user_client"})
def submit_metadata_to_db(context: OpExecutionContext, database: nmdc.Database) -> str:
    client: RuntimeApiUserClient = context.resources.runtime_api_user_client
    response = client.submit_metadata(database)
    body = response.json()
    return body["detail"]["run_id"]


@op(required_resource_keys={"mongo"})
def get_changesheet_in(context) -> ChangesheetIn:
    mdb: MongoDatabase = context.resources.mongo.db
    object_id = context.op_config.get("object_id")
    mdb_fs = GridFS(mdb)
    grid_out = mdb_fs.get(object_id)
    return ChangesheetIn(
        name=grid_out.filename, content_type=grid_out.content_type, text=grid_out.read()
    )


@op(required_resource_keys={"mongo"})
def perform_changesheet_updates(context, sheet_in: ChangesheetIn):
    mdb: MongoDatabase = context.resources.mongo.db
    op_id = context.op_config.get("operation_id")
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
    op_doc = op.model_dump(exclude_unset=True)
    mdb.operations.replace_one({"id": op_id}, op_doc)
    return ["/operations/" + op_doc["id"]]


@op(required_resource_keys={"runtime_api_site_client"})
def get_json_in(context):
    """
    TODO: Document this function.
    """
    object_id = context.op_config.get("object_id")
    client: RuntimeApiSiteClient = context.resources.runtime_api_site_client
    rv = client.get_object_bytes(object_id)
    if rv.status_code != 200:
        raise Failure(
            description=f"error code {rv.status_code} for {rv.request.url}: {rv.text}"
        )
    return rv.json()


@op(required_resource_keys={"runtime_api_site_client", "mongo"})
def perform_mongo_updates(context, json_in):
    """
    TODO: Document this function.
    """
    mongo = context.resources.mongo
    client: RuntimeApiSiteClient = context.resources.runtime_api_site_client
    op_id = context.op_config.get("operation_id")

    docs = json_in
    docs, _ = specialize_activity_set_docs(docs)
    context.log.debug(f"{docs}")

    rv = validate_json(
        docs, mongo.db
    )  # use *exact* same check as /metadata/json:validate
    if rv["result"] == "errors":
        raise Failure(str(rv["detail"]))

    # TODO containing op `perform_mongo_updates` needs test coverage, as below line had trivial bug.
    #   ref: https://github.com/microbiomedata/nmdc-runtime/issues/631
    add_docs_result = _add_schema_docs_with_or_without_replacement(mongo, docs)
    op_patch = UpdateOperationRequest(
        done=True,
        result=add_docs_result,
        metadata={"done_at": datetime.now(timezone.utc).isoformat(timespec="seconds")},
    )
    op_doc = client.update_operation(op_id, op_patch).json()
    return ["/operations/" + op_doc["id"]]


def _add_schema_docs_with_or_without_replacement(
    mongo: MongoDBResource, docs: Dict[str, list]
):
    """
    TODO: Document this function.
    """
    coll_index_on_id_map = schema_collection_has_index_on_id(mongo.db)
    if all(coll_index_on_id_map[coll] for coll in docs.keys()):
        replace = True
    elif all(not coll_index_on_id_map[coll] for coll in docs.keys()):
        # FIXME: XXX: This is a hack because e.g. <https://w3id.org/nmdc/FunctionalAnnotationAggMember>
        # documents should be unique with compound key (metagenome_annotation_id, gene_function_id)
        # and yet this is not explicit in the schema. One potential solution is to auto-generate an `id`
        # as a deterministic hash of the compound key.
        #
        # For now, decision is to potentially re-insert "duplicate" documents, i.e. to interpret
        # lack of `id` as lack of unique document identity for de-duplication.
        replace = False  # wasting time trying to upsert by `id`.
    else:
        colls_not_id_indexed = [
            coll for coll in docs.keys() if not coll_index_on_id_map[coll]
        ]
        colls_id_indexed = [coll for coll in docs.keys() if coll_index_on_id_map[coll]]
        raise Failure(
            "Simultaneous addition of non-`id`ed collections and `id`-ed collections"
            " is not supported at this time."
            f"{colls_not_id_indexed=} ; {colls_id_indexed=}"
        )
    op_result = mongo.add_docs(docs, validate=False, replace=replace)

    # Translate the operation result into a dictionary in which each item's key is a collection name
    # and each item's value is the corresponding bulk API result (excluding the "upserted" field).
    return {
        collection_name: dissoc(bulk_write_result.bulk_api_result, "upserted")
        for collection_name, bulk_write_result in op_result.items()
    }


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


@op
def nmdc_schema_database_export_filename(study: Dict[str, Any]) -> str:
    source_id = None
    if "id" in study:
        source_id = study["id"]
    elif "studyGoldId" in study:
        source_id = study["studyGoldId"]
    return f"database_from_{source_id}.json"


@op
def nmdc_schema_object_to_dict(object: YAMLRoot) -> Dict[str, Any]:
    return as_simple_dict(object)


@op(required_resource_keys={"mongo"}, config_schema={"username": str})
def export_json_to_drs(
    context: OpExecutionContext, data: Dict, filename: str, description: str = ""
) -> List[str]:
    mdb = context.resources.mongo.db
    username = context.op_config.get("username")
    content = json.dumps(data)
    sha256hash = hash_from_str(content, "sha256")
    drs_object = mdb.objects.find_one(
        {"checksums": {"$elemMatch": {"type": "sha256", "checksum": sha256hash}}}
    )
    if drs_object is None:
        drs_object = persist_content_and_get_drs_object(
            content=content,
            username=username,
            filename=filename,
            content_type="application/json",
            description=description,
            id_ns="export-json",
        )
    context.log_event(
        AssetMaterialization(
            asset_key=filename,
            description=description,
            metadata={
                "drs_object_id": MetadataValue.text(drs_object["id"]),
                "json": MetadataValue.json(data),
            },
        )
    )
    return ["/objects/" + drs_object["id"]]


def unique_field_values(docs: List[Dict[str, Any]], field: str):
    return {doc[field] for doc in docs if field in doc}


@op
def get_csv_rows_from_url(url: Optional[str]) -> List[Dict]:
    """Download and parse a CSV file from a remote URL.

    This method fetches data from the given URL and parses that data as CSV. The parsed data
    is returned as a list (each element corresponds to a row) of dicts (each key is a column
    name and the value is the corresponding cell value). The dict will *not* contain keys
    for columns where the cell was empty.

    :param url: Url to fetch and parse
    :return: List[Dict]
    """
    if not url:
        return []

    response = requests.get(url)
    response.raise_for_status()

    reader = csv.DictReader(response.text.splitlines())
    # Collect all the rows into a list of dicts while stripping out (valfilter) cells where the
    # value is an empty string (identity returns a Falsy value).
    return [valfilter(identity, row) for row in reader]


@op
def get_df_from_url(url: str) -> pd.DataFrame:
    """Download and return a pandas DataFrame from the URL of a TSV file.

    :param url: raw URL of the TSV file to be downloaded as a DataFrame
    :return: pandas DataFrame of TSV data
    """
    if not url:
        return pd.DataFrame()

    response = requests.get(url)
    response.raise_for_status()

    # Using Pandas read_csv to directly read the file-like object
    df = pd.read_csv(url, delimiter="\t")

    return df


@op
def post_submission_portal_biosample_ingest_record_stitching_filename(
    nmdc_study_id: str,
) -> str:
    filename = nmdc_study_id_to_filename(nmdc_study_id)
    return f"missing_database_records_for_{filename}.json"


@op
def log_database_ids(
    context: OpExecutionContext,
    database: nmdc.Database,
) -> None:
    """Log the IDs of the database."""
    database_dict = as_simple_dict(database)
    message = ""
    for collection_name, collection in database_dict.items():
        if not isinstance(collection, list):
            continue
        message += f"{collection_name} ({len(collection)}):\n"
        if len(collection) < 10:
            message += "\n".join(f"  {doc['id']}" for doc in collection)
        else:
            message += "\n".join(f"  {doc['id']}" for doc in collection[:4])
            message += f"\n  ... {len(collection) - 8} more\n"
            message += "\n".join(f"  {doc['id']}" for doc in collection[-4:])
        message += "\n"
    if message:
        context.log.info(message)


@op(
    description="Render free text through the Dagit UI",
    out=Out(description="Text content rendered through Dagit UI"),
)
def render_text(context: OpExecutionContext, text: Any):
    """
    Renders content as a Dagster Asset in the Dagit UI.

    This operation creates a Dagster Asset with the provided content, making it
    visible in the Dagit UI for easy viewing and sharing.

    Args:
        context: The execution context
        text: The content to render (can be a string or a dictionary that will be converted to JSON)

    Returns:
        The same content that was provided as input
    """
    # Convert dictionary to formatted JSON string if needed
    if isinstance(text, dict):
        import json

        content = json.dumps(text, indent=2)
        file_extension = "json"
        hash_text = json.dumps(text, sort_keys=True)[:20]  # For consistent hashing
    else:
        content = str(text)  # Convert to string in case it's not already
        file_extension = "txt"
        hash_text = content[:20]

    filename = f"rendered_text_{context.run_id}.{file_extension}"
    file_path = os.path.join(context.instance.storage_directory(), filename)

    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    with open(file_path, "w") as f:
        f.write(content)

    context.log_event(
        AssetMaterialization(
            asset_key=f"rendered_text_{hash_from_str(hash_text, 'md5')[:8]}",
            description="Rendered Content",
            metadata={
                "file_path": MetadataValue.path(file_path),
                "content": MetadataValue.text(content),
            },
        )
    )

    return Output(text)


def send_slack_message(
    context: OpExecutionContext,
    *,
    text: str,
    channel_name_or_id: str = config.DAGSTER_SLACK_CHANNEL,
    raise_on_error: bool = False,
) -> bool:
    """
    Sends a message to a Slack channel and returns a boolean indicating whether it was sent.

    By default, if we fail to send the message, we just log an error and return `False`.
    However, if the caller has set `raise_on_error=True` and we fail to send the message,
    we raise an exception.
    """
    try:
        formatted_text = f"{text} _(Environment: `{config.DAGSTER_ENVIRONMENT}`)_"
        slack_web_client = context.resources.slack_resource.get_client()
        slack_web_client.chat_postMessage(
            channel=channel_name_or_id,
            text=formatted_text,
        )
    except Exception as error:
        if raise_on_error:
            raise
        else:
            context.log.exception("Failed to send Slack message.", exc_info=error)
            return False
    return True


@op(required_resource_keys={"slack_resource"})
def send_example_slack_message_op(context: OpExecutionContext):
    """Sends an example Slack message, to confirm the Slack integration works."""
    send_slack_message(
        context=context,
        text=r":robot_face: Hello from Dagster. This is a test.",
        raise_on_error=True,
    )
