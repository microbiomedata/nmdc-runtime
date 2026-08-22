import csv
import json
import os
import subprocess
from collections import defaultdict
from datetime import datetime, timezone
from importlib.metadata import version
from io import BytesIO
from pprint import pformat

from typing import Optional, Set, Tuple
from zipfile import ZipFile
import pandas as pd
import requests
from toolz import dissoc

from bson import json_util
from dagster import (
    Any,
    AssetKey,
    AssetMaterialization,
    Dict,
    Failure,
    List,
    MetadataValue,
    OpExecutionContext,
    Out,
    Output,
    RetryPolicy,
    RetryRequested,
    String,
    op,
    Field,
    Permissive,
)
from gridfs import GridFS
from linkml_runtime.utils.dictutils import as_simple_dict
from linkml_runtime.utils.yamlutils import YAMLRoot
from nmdc_runtime.api.db.mongo import validate_json
from nmdc_runtime.api.core.idgen import generate_one_id
from nmdc_runtime.api.core.metadata import (
    _validate_changesheet,
    df_from_sheet_in,
    get_collection_for_id,
    map_id_to_collection,
)
from nmdc_runtime.api.core.util import dotted_path_for, hash_from_str, json_clean, now
from nmdc_runtime.api.endpoints.util import persist_content_and_get_drs_object
from nmdc_runtime.api.endpoints.find import find_study_by_id
from nmdc_runtime.api.models.job import Job, JobOperationMetadata
from nmdc_runtime.api.models.metadata import ChangesheetIn
from nmdc_runtime.api.models.operation import (
    ObjectPutMetadata,
    Operation,
    UpdateOperationRequest,
)
from nmdc_runtime.api.models.run import (
    RunEventType,
    RunSummary,
    _add_run_complete_event,
)
from nmdc_runtime.api.models.util import ResultT
from nmdc_runtime.site.export.ncbi_xml import NCBISubmissionXML
from nmdc_runtime.site.export.ncbi_xml_utils import (
    fetch_data_objects_from_biosamples,
    fetch_nucleotide_sequencing_from_biosamples,
    fetch_library_preparation_from_biosamples,
)
from nmdc_runtime.site.resources import (
    GoldApiClient,
    RuntimeApiSiteClient,
    RuntimeApiUserClient,
    NeonApiClient,
    MongoDB as MongoDBResource,
)
from nmdc_runtime.site.translation.gold_translator import GoldStudyTranslator
from nmdc_runtime.site.translation.neon_soil_translator import NeonSoilDataTranslator
from nmdc_runtime.site.translation.neon_benthic_translator import (
    NeonBenthicDataTranslator,
)
from nmdc_runtime.site.translation.neon_surface_water_translator import (
    NeonSurfaceWaterDataTranslator,
)
from nmdc_runtime.site.repair.database_updater import DatabaseUpdater
from nmdc_runtime.site.util import (
    schema_collection_has_index_on_id,
    nmdc_study_id_to_filename,
    get_instruments_by_id,
)
from nmdc_runtime.util import (
    pluralize,
    specialize_activity_set_docs,
)
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


@op(required_resource_keys={"mongo"})
def get_operation(context):
    mdb = context.resources.mongo.db
    id_op = context.op_config.get("operation_id")
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


@op(required_resource_keys={"runtime_api_user_client"})
def poll_for_run_completion(context: OpExecutionContext, run_id: str) -> RunSummary:
    client: RuntimeApiUserClient = context.resources.runtime_api_user_client
    response = client.get_run_info(run_id)
    body = RunSummary.parse_obj(response.json())
    context.log.info(body.status)
    if body.status != RunEventType.COMPLETE:
        raise RetryRequested(max_retries=12, seconds_to_wait=10)
    return body


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
    ops = [op.model_dump() for op in client.list_operations({"filter": filter_})]
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
def construct_jobs(context: OpExecutionContext) -> List[Job]:
    mdb: MongoDatabase = context.resources.mongo.db
    docs = [
        dict(**base, id=generate_one_id(mdb, "jobs"), created_at=now())
        for base in context.op_config["base_jobs"]
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


@op(
    config_schema={
        "study_id": str,
        "study_type": str,
        "gold_nmdc_instrument_mapping_file_url": str,
        "include_field_site_info": bool,
        "enable_biosample_filtering": bool,
    },
    out={
        "study_id": Out(str),
        "study_type": Out(str),
        "gold_nmdc_instrument_mapping_file_url": Out(str),
        "include_field_site_info": Out(bool),
        "enable_biosample_filtering": Out(bool),
    },
)
def get_gold_study_pipeline_inputs(
    context: OpExecutionContext,
) -> Tuple[str, str, str, bool, bool]:
    return (
        context.op_config["study_id"],
        context.op_config["study_type"],
        context.op_config["gold_nmdc_instrument_mapping_file_url"],
        context.op_config["include_field_site_info"],
        context.op_config["enable_biosample_filtering"],
    )


@op(required_resource_keys={"gold_api_client"})
def gold_biosamples_by_study(
    context: OpExecutionContext, study_id: str
) -> List[Dict[str, Any]]:
    client: GoldApiClient = context.resources.gold_api_client
    return client.fetch_biosamples_by_study(study_id)


@op(required_resource_keys={"gold_api_client"})
def gold_projects_by_study(
    context: OpExecutionContext, study_id: str
) -> List[Dict[str, Any]]:
    client: GoldApiClient = context.resources.gold_api_client
    return client.fetch_projects_by_study(study_id)


@op(required_resource_keys={"gold_api_client"})
def gold_analysis_projects_by_study(
    context: OpExecutionContext, study_id: str
) -> List[Dict[str, Any]]:
    client: GoldApiClient = context.resources.gold_api_client
    return client.fetch_analysis_projects_by_study(study_id)


@op(required_resource_keys={"gold_api_client"})
def gold_study(context: OpExecutionContext, study_id: str) -> Dict[str, Any]:
    client: GoldApiClient = context.resources.gold_api_client
    return client.fetch_study(study_id)


@op(required_resource_keys={"runtime_api_site_client"})
def nmdc_schema_database_from_gold_study(
    context: OpExecutionContext,
    study: Dict[str, Any],
    study_type: str,
    projects: List[Dict[str, Any]],
    biosamples: List[Dict[str, Any]],
    analysis_projects: List[Dict[str, Any]],
    gold_nmdc_instrument_map_df: pd.DataFrame,
    include_field_site_info: bool,
    enable_biosample_filtering: bool,
) -> nmdc.Database:
    client: RuntimeApiSiteClient = context.resources.runtime_api_site_client

    def id_minter(*args, **kwargs):
        response = client.mint_id(*args, **kwargs)
        return response.json()

    translator = GoldStudyTranslator(
        study,
        study_type,
        biosamples,
        projects,
        analysis_projects,
        gold_nmdc_instrument_map_df,
        include_field_site_info,
        enable_biosample_filtering,
        id_minter=id_minter,
    )
    database = translator.get_database()
    return database




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


@op(
    description="NCBI Submission XML file rendered in a Dagster Asset",
    out=Out(description="XML content rendered through Dagit UI"),
)
def ncbi_submission_xml_asset(context: OpExecutionContext, data: str):
    filename = "ncbi_submission.xml"
    file_path = os.path.join(context.instance.storage_directory(), filename)

    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    with open(file_path, "w") as f:
        f.write(data)

    context.log_event(
        AssetMaterialization(
            asset_key="ncbi_submission_xml",
            description="NCBI Submission XML Data",
            metadata={
                "file_path": MetadataValue.path(file_path),
                "xml": MetadataValue.text(data),
            },
        )
    )

    return Output(data)


def unique_field_values(docs: List[Dict[str, Any]], field: str):
    return {doc[field] for doc in docs if field in doc}


@op(config_schema={"dna_sample_ids": [str]})
def get_neon_soil_sample_filter(context: OpExecutionContext) -> Set[str]:
    dna_sample_ids = set(context.op_config["dna_sample_ids"])
    context.log.info(f"Filtering to {len(dna_sample_ids)} dnaSampleIDs")
    return dna_sample_ids


@op(config_schema={"mms_data_product": dict})
def get_neon_pipeline_mms_data_product(context: OpExecutionContext) -> dict:
    return context.op_config["mms_data_product"]


@op(config_schema={"sls_data_product": dict})
def get_neon_pipeline_sls_data_product(context: OpExecutionContext) -> dict:
    return context.op_config["sls_data_product"]


@op(config_schema={"benthic_data_product": dict})
def get_neon_pipeline_benthic_data_product(context: OpExecutionContext) -> dict:
    return context.op_config["benthic_data_product"]


@op(config_schema={"surface_water_data_product": dict})
def get_neon_pipeline_surface_water_data_product(context: OpExecutionContext) -> dict:
    return context.op_config["surface_water_data_product"]


@op(required_resource_keys={"neon_api_client"})
def neon_data_by_product(
    context: OpExecutionContext, data_product: dict
) -> Dict[str, pd.DataFrame]:
    df_dict = {}
    client: NeonApiClient = context.resources.neon_api_client

    product_id = data_product["product_id"]
    product_tables = data_product["product_tables"]

    product_table_list = [t.strip() for t in product_tables.split(",")]
    product = client.fetch_product_by_id(product_id)
    for table_name in product_table_list:
        df = pd.DataFrame()
        for site in product["data"]["siteCodes"]:
            for data_url in site["availableDataUrls"]:
                data_files = client.request(data_url)
                for file in data_files["data"]["files"]:
                    if table_name in file["name"] and "expanded" in file["name"]:
                        current_df = pd.read_csv(file["url"])
                        df = pd.concat([df, current_df], ignore_index=True)
        df_dict[table_name] = df

    return df_dict


@op(required_resource_keys={"runtime_api_site_client"})
def nmdc_schema_database_from_neon_soil_data(
    context: OpExecutionContext,
    mms_data: Dict[str, pd.DataFrame],
    sls_data: Dict[str, pd.DataFrame],
    site_code_mapping: Dict[str, str],
    neon_envo_mappings_file: pd.DataFrame,
    neon_raw_data_file_mappings_file: pd.DataFrame,
    neon_nmdc_instrument_mapping_file: pd.DataFrame,
    allowed_dna_sample_ids: Optional[Set[str]],
) -> nmdc.Database:
    client: RuntimeApiSiteClient = context.resources.runtime_api_site_client

    def id_minter(*args, **kwargs):
        response = client.mint_id(*args, **kwargs)
        return response.json()

    translator = NeonSoilDataTranslator(
        mms_data,
        sls_data,
        neon_envo_mappings_file,
        neon_raw_data_file_mappings_file,
        site_code_mapping,
        neon_nmdc_instrument_mapping_file,
        allowed_dna_sample_ids=allowed_dna_sample_ids,
        id_minter=id_minter,
    )

    database = translator.get_database()
    return database


@op(required_resource_keys={"runtime_api_site_client"})
def nmdc_schema_database_from_neon_benthic_data(
    context: OpExecutionContext,
    benthic_data: Dict[str, pd.DataFrame],
    site_code_mapping: Dict[str, str],
    neon_envo_mappings_file: pd.DataFrame,
    neon_raw_data_file_mappings_file: pd.DataFrame,
    neon_nmdc_instrument_mapping_file: pd.DataFrame,
) -> nmdc.Database:
    client: RuntimeApiSiteClient = context.resources.runtime_api_site_client

    def id_minter(*args, **kwargs):
        response = client.mint_id(*args, **kwargs)
        return response.json()

    translator = NeonBenthicDataTranslator(
        benthic_data,
        site_code_mapping,
        neon_envo_mappings_file,
        neon_raw_data_file_mappings_file,
        neon_nmdc_instrument_mapping_file,
        id_minter=id_minter,
    )

    database = translator.get_database()
    return database


@op(required_resource_keys={"runtime_api_site_client"})
def nmdc_schema_database_from_neon_surface_water_data(
    context: OpExecutionContext,
    surface_water_data: Dict[str, pd.DataFrame],
    site_code_mapping: Dict[str, str],
    neon_envo_mappings_file: pd.DataFrame,
    neon_raw_data_file_mappings_file: pd.DataFrame,
    neon_nmdc_instrument_mapping_file: pd.DataFrame,
) -> nmdc.Database:
    client: RuntimeApiSiteClient = context.resources.runtime_api_site_client

    def id_minter(*args, **kwargs):
        response = client.mint_id(*args, **kwargs)
        return response.json()

    translator = NeonSurfaceWaterDataTranslator(
        surface_water_data,
        site_code_mapping,
        neon_envo_mappings_file,
        neon_raw_data_file_mappings_file,
        neon_nmdc_instrument_mapping_file,
        id_minter=id_minter,
    )

    database = translator.get_database()
    return database


@op(
    out={
        "neon_envo_mappings_file_url": Out(),
        "neon_raw_data_file_mappings_file_url": Out(),
        "neon_nmdc_instrument_mapping_file_url": Out(),
    }
)
def get_neon_pipeline_inputs(
    neon_envo_mappings_file_url: str,
    neon_raw_data_file_mappings_file_url: str,
    neon_nmdc_instrument_mapping_file_url: str,
) -> Tuple[str, str, str]:
    return (
        neon_envo_mappings_file_url,
        neon_raw_data_file_mappings_file_url,
        neon_nmdc_instrument_mapping_file_url,
    )


@op
def nmdc_schema_database_export_filename_neon() -> str:
    return "database_from_neon_metadata.json"


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
def site_code_mapping() -> dict:
    endpoint = "https://data.neonscience.org/api/v0/sites/"
    response = requests.get(endpoint)
    if response.status_code == 200:
        sites_data = response.json()
        site_code_mapping = {
            site["siteCode"]: f"USA: {site['stateName']}, {site['siteName']}".replace(
                " NEON", ""
            )
            for site in sites_data["data"]
        }
        return site_code_mapping
    else:
        raise Exception(
            f"Failed to fetch site data from {endpoint}. Status code: {response.status_code}, Content: {response.content}"
        )


@op(config_schema={"nmdc_study_id": str}, required_resource_keys={"mongo"})
def get_ncbi_export_pipeline_study(context: OpExecutionContext) -> Any:
    nmdc_study = find_study_by_id(
        context.op_config["nmdc_study_id"], context.resources.mongo.db
    )
    return nmdc_study


@op(
    config_schema={
        "nmdc_ncbi_attribute_mapping_file_url": str,
        "ncbi_submission_metadata": Field(
            Permissive(
                {
                    "organization": String,
                }
            ),
            is_required=True,
            description="General metadata about the NCBI submission.",
        ),
        "ncbi_biosample_metadata": Field(
            Permissive(
                {
                    "organism_name": String,
                }
            ),
            is_required=True,
            description="Metadata for one or many NCBI BioSample in the Submission.",
        ),
    },
    out=Out(Dict),
)
def get_ncbi_export_pipeline_inputs(context: OpExecutionContext) -> str:
    nmdc_ncbi_attribute_mapping_file_url = context.op_config[
        "nmdc_ncbi_attribute_mapping_file_url"
    ]
    ncbi_submission_metadata = context.op_config.get("ncbi_submission_metadata", {})
    ncbi_biosample_metadata = context.op_config.get("ncbi_biosample_metadata", {})

    return {
        "nmdc_ncbi_attribute_mapping_file_url": nmdc_ncbi_attribute_mapping_file_url,
        "ncbi_submission_metadata": ncbi_submission_metadata,
        "ncbi_biosample_metadata": ncbi_biosample_metadata,
    }


@op(required_resource_keys={"mongo"})
def get_data_objects_from_biosamples(context: OpExecutionContext, biosamples: list):
    mdb = context.resources.mongo.db
    alldocs_collection = mdb["alldocs"]
    data_object_set = mdb["data_object_set"]
    biosample_data_objects = fetch_data_objects_from_biosamples(
        alldocs_collection, data_object_set, biosamples
    )
    return biosample_data_objects


@op(required_resource_keys={"mongo"})
def get_nucleotide_sequencing_from_biosamples(
    context: OpExecutionContext, biosamples: list
):
    mdb = context.resources.mongo.db
    alldocs_collection = mdb["alldocs"]
    data_generation_set = mdb["data_generation_set"]
    biosample_omics_processing = fetch_nucleotide_sequencing_from_biosamples(
        alldocs_collection, data_generation_set, biosamples
    )
    return biosample_omics_processing


@op(required_resource_keys={"mongo"})
def get_library_preparation_from_biosamples(
    context: OpExecutionContext, biosamples: list
):
    mdb = context.resources.mongo.db
    alldocs_collection = mdb["alldocs"]
    material_processing_set = mdb["material_processing_set"]
    biosample_lib_prep = fetch_library_preparation_from_biosamples(
        alldocs_collection, material_processing_set, biosamples
    )
    return biosample_lib_prep


@op(required_resource_keys={"mongo"})
def get_aggregated_pooled_biosamples(context: OpExecutionContext, biosamples: list):
    from nmdc_runtime.site.export.ncbi_xml_utils import check_pooling_for_biosamples

    mdb = context.resources.mongo.db
    material_processing_set = mdb["material_processing_set"]
    pooled_biosamples_data = check_pooling_for_biosamples(
        material_processing_set, biosamples
    )

    # Fetch ProcessedSample names from database
    processed_sample_ids = set()
    for biosample_id, pooling_info in pooled_biosamples_data.items():
        if pooling_info and pooling_info.get("processed_sample_id"):
            processed_sample_ids.add(pooling_info["processed_sample_id"])

    # Query database for ProcessedSample names
    if processed_sample_ids:
        processed_sample_set = mdb["processed_sample_set"]
        cursor = processed_sample_set.find(
            {"id": {"$in": list(processed_sample_ids)}}, {"id": 1, "name": 1}
        )
        processed_samples = {doc["id"]: doc.get("name", "") for doc in cursor}

        # Update pooled_biosamples_data with ProcessedSample names
        for biosample_id, pooling_info in pooled_biosamples_data.items():
            if pooling_info and pooling_info.get("processed_sample_id"):
                processed_sample_id = pooling_info["processed_sample_id"]
                if processed_sample_id in processed_samples:
                    pooling_info["processed_sample_name"] = processed_samples[
                        processed_sample_id
                    ]

    return pooled_biosamples_data


@op(required_resource_keys={"mongo"})
def get_all_instruments(context: OpExecutionContext) -> dict[str, dict]:
    mdb = context.resources.mongo.db
    return get_instruments_by_id(mdb)


@op(required_resource_keys={"mongo"})
def get_instrument_ids_by_model(context: OpExecutionContext) -> dict[str, str]:
    mdb = context.resources.mongo.db
    instruments_by_id = get_instruments_by_id(mdb)
    instruments_by_model: dict[str, str] = {}
    for inst_id, instrument in instruments_by_id.items():
        model = instrument.get("model")
        if model is None:
            context.log.warning(f"Instrument {inst_id} has no model.")
            continue
        if model in instruments_by_model:
            context.log.warning(f"Instrument model {model} is not unique.")
        instruments_by_model[model] = inst_id
    context.log.info("Instrument models: %s", pformat(instruments_by_model))
    return instruments_by_model


@op
def ncbi_submission_xml_from_nmdc_study(
    context: OpExecutionContext,
    nmdc_study: Any,
    ncbi_exporter_metadata: dict,
    biosamples: list,
    omics_processing_records: list,
    data_object_records: list,
    library_preparation_records: list,
    all_instruments: dict,
    pooled_biosamples_data: dict,
) -> str:
    ncbi_exporter = NCBISubmissionXML(nmdc_study, ncbi_exporter_metadata)
    ncbi_xml = ncbi_exporter.get_submission_xml(
        biosamples,
        omics_processing_records,
        data_object_records,
        library_preparation_records,
        all_instruments,
        pooled_biosamples_data,
    )
    return ncbi_xml


@op
def post_submission_portal_biosample_ingest_record_stitching_filename(
    nmdc_study_id: str,
) -> str:
    filename = nmdc_study_id_to_filename(nmdc_study_id)
    return f"missing_database_records_for_{filename}.json"


@op(
    config_schema={
        "nmdc_study_id": str,
        "gold_nmdc_instrument_mapping_file_url": str,
        "include_field_site_info": bool,
        "enable_biosample_filtering": bool,
    },
    out={
        "nmdc_study_id": Out(str),
        "gold_nmdc_instrument_mapping_file_url": Out(str),
        "include_field_site_info": Out(bool),
        "enable_biosample_filtering": Out(bool),
    },
)
def get_database_updater_inputs(
    context: OpExecutionContext,
) -> Tuple[str, str, bool, bool]:
    return (
        context.op_config["nmdc_study_id"],
        context.op_config["gold_nmdc_instrument_mapping_file_url"],
        context.op_config["include_field_site_info"],
        context.op_config["enable_biosample_filtering"],
    )


@op(
    required_resource_keys={
        "runtime_api_user_client",
        "runtime_api_site_client",
        "gold_api_client",
    }
)
def generate_data_generation_set_post_biosample_ingest(
    context: OpExecutionContext,
    nmdc_study_id: str,
    gold_nmdc_instrument_map_df: pd.DataFrame,
    include_field_site_info: bool,
    enable_biosample_filtering: bool,
) -> nmdc.Database:
    runtime_api_user_client: RuntimeApiUserClient = (
        context.resources.runtime_api_user_client
    )
    runtime_api_site_client: RuntimeApiSiteClient = (
        context.resources.runtime_api_site_client
    )
    gold_api_client: GoldApiClient = context.resources.gold_api_client

    database_updater = DatabaseUpdater(
        runtime_api_user_client,
        runtime_api_site_client,
        gold_api_client,
        nmdc_study_id,
        gold_nmdc_instrument_map_df,
        include_field_site_info,
        enable_biosample_filtering,
    )
    database = (
        database_updater.generate_data_generation_set_records_from_gold_api_for_study()
    )

    return database


@op(
    required_resource_keys={
        "runtime_api_user_client",
        "runtime_api_site_client",
        "gold_api_client",
    }
)
def generate_biosample_set_for_nmdc_study_from_gold(
    context: OpExecutionContext,
    nmdc_study_id: str,
    gold_nmdc_instrument_map_df: pd.DataFrame,
    include_field_site_info: bool = False,
    enable_biosample_filtering: bool = False,
) -> nmdc.Database:
    runtime_api_user_client: RuntimeApiUserClient = (
        context.resources.runtime_api_user_client
    )
    runtime_api_site_client: RuntimeApiSiteClient = (
        context.resources.runtime_api_site_client
    )
    gold_api_client: GoldApiClient = context.resources.gold_api_client

    database_updater = DatabaseUpdater(
        runtime_api_user_client,
        runtime_api_site_client,
        gold_api_client,
        nmdc_study_id,
        gold_nmdc_instrument_map_df,
        include_field_site_info,
        enable_biosample_filtering,
    )
    database = database_updater.generate_biosample_set_from_gold_api_for_study()

    return database


@op(
    required_resource_keys={
        "runtime_api_user_client",
        "runtime_api_site_client",
        "gold_api_client",
    },
    out=Out(Any),
)
def run_script_to_update_insdc_biosample_identifiers(
    context: OpExecutionContext,
    nmdc_study_id: str,
    gold_nmdc_instrument_map_df: pd.DataFrame,
    include_field_site_info: bool,
    enable_biosample_filtering: bool,
):
    """Generates a MongoDB update script to add INSDC biosample identifiers to biosamples.

    This op uses the DatabaseUpdater to generate a script that can be used to update biosample
    records with INSDC identifiers obtained from GOLD.

    Args:
        context: The execution context
        nmdc_study_id: The NMDC study ID for which to generate the update script
        gold_nmdc_instrument_map_df: A dataframe mapping GOLD instrument IDs to NMDC instrument set records

    Returns:
        A dictionary or list of dictionaries containing the MongoDB update script(s)
    """
    runtime_api_user_client: RuntimeApiUserClient = (
        context.resources.runtime_api_user_client
    )
    runtime_api_site_client: RuntimeApiSiteClient = (
        context.resources.runtime_api_site_client
    )
    gold_api_client: GoldApiClient = context.resources.gold_api_client

    database_updater = DatabaseUpdater(
        runtime_api_user_client,
        runtime_api_site_client,
        gold_api_client,
        nmdc_study_id,
        gold_nmdc_instrument_map_df,
        include_field_site_info,
        enable_biosample_filtering,
    )
    update_script = database_updater.queries_run_script_to_update_insdc_identifiers()

    if isinstance(update_script, list):
        total_updates = sum(len(item.get("updates", [])) for item in update_script)
    else:
        total_updates = len(update_script.get("updates", []))
    context.log.info(
        f"Generated update script for study {nmdc_study_id} with {total_updates} updates"
    )

    return update_script


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
