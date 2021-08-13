"""
Translating an export of the JGI GOLD [1] database of SQL tables to the NMDC database JSON schema.

[1] Genomes OnLine Database (GOLD) <https://gold.jgi.doe.gov/>.
"""

from io import BytesIO
import json
import os
from subprocess import Popen, PIPE, STDOUT, CalledProcessError
from zipfile import ZipFile

import pymongo.database
from git_root import git_root
from dagster import (
    solid,
    AssetMaterialization,
    Output,
    EventMetadata,
    Failure,
    OutputDefinition,
    AssetKey,
)
from toolz import get_in
from pymongo import MongoClient

from nmdc_runtime.api.models.job import JobOperationMetadata
from nmdc_runtime.api.models.operation import Operation, ResultT
from nmdc_runtime.resources.core import RuntimeApiSiteClient

# resources needed for GOLD ETL
from nmdc_schema import nmdc
from nmdc_runtime.lib.nmdc_etl_class import NMDC_ETL
import nmdc_runtime.lib.nmdc_dataframes as nmdc_dataframes


def run_and_log(shell_cmd, context):
    process = Popen(shell_cmd, shell=True, stdout=PIPE, stderr=STDOUT)
    for line in process.stdout:
        context.log.info(line.decode())
    retcode = process.wait()
    if retcode:
        raise CalledProcessError(retcode, process.args)


@solid(
    output_defs=[
        OutputDefinition(
            str,
            name="merged_data_path",
            description="path to TSV merging of source metadata",
        )
    ]
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


@solid(
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


@solid(required_resource_keys={"runtime_api_site_client", "mongo"})
def produce_curated_db(context, op: Operation):
    client: RuntimeApiSiteClient = context.resources.runtime_api_site_client
    mdb: pymongo.database.Database = context.resources.mongo.db
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


@solid
def load_nmdc_etl_class(context) -> NMDC_ETL:

    # build instance of NMDC_ETL class
    etl = NMDC_ETL(
        merged_data_file=context.solid_config["data_file"],
        data_source_spec_file=context.solid_config["spec_file"],
        sssom_file="",
    )
    return etl


@solid
def transform_study(context, nmdc_etl: NMDC_ETL) -> tuple:
    # return {"study_set": nmdc_etl.transform_study()}
    return ("gold.study_set", nmdc_etl.transform_study())


@solid
def transform_omics_processing(context, nmdc_etl: NMDC_ETL) -> tuple:
    return ("gold.omics_processing_set", nmdc_etl.transform_omics_processing())


@solid
def transform_biosample(context, nmdc_etl: NMDC_ETL) -> tuple:
    return ("gold.biosample_set", nmdc_etl.transform_biosample())


@solid
def get_mongo_db(context) -> pymongo.database.Database:
    host = context.solid_config["host"]
    username = context.solid_config["username"]
    password = context.solid_config["password"]
    dbname = context.solid_config["dbname"]

    client = MongoClient(host=host, username=username, password=password)
    return client[dbname]


@solid
def load_mongo_collection(context, mongo_db: pymongo.database.Database, data: tuple):
    collecton_name = data[0]  # get collection name
    documents = data[1]  # get data portion of tuple
    collecton = mongo_db[collecton_name]  # get mongo collection

    # drop collection if exists
    collecton.drop()

    # insert data
    collecton.insert(documents)


### TESTING PIPELINE
if __name__ == "__main__":
    from dagster import pipeline
    from dagster import execute_pipeline

    @pipeline
    def gold_pipeline1():
        # load_merged_data_source()
        db = get_mongo_db()
        nmdc_etl = load_nmdc_etl_class()
        gold_study = transform_study(nmdc_etl)
        gold_omics_processing = transform_omics_processing(nmdc_etl)
        gold_biosample = transform_biosample(nmdc_etl)

        # load data into mongo
        load_mongo_collection(db, gold_study)
        load_mongo_collection(db, gold_omics_processing)
        load_mongo_collection(db, gold_biosample)

    run_config1 = {
        "solids": {
            "load_nmdc_etl_class": {
                "config": {
                    "data_file": git_root(
                        "metadata-translation/src/data/nmdc_merged_data.tsv.zip"
                    ),
                    "sssom_map_file": "",
                    "spec_file": git_root("nmdc_runtime/lib/nmdc_data_source.yaml"),
                }
            },
            "get_mongo_db": {
                "config": {
                    "host": os.getenv("MONGO_HOST"),
                    "username": os.getenv("MONGO_USERNAME"),
                    "password": os.getenv("MONGO_PASSWORD"),
                    "dbname": "nmdc_etl_staging",
                }
            },
        },
    }

    result = execute_pipeline(gold_pipeline1, run_config=run_config1)
