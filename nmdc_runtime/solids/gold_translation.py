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

from nmdc_runtime.api.models.job import JobOperationMetadata
from nmdc_runtime.api.models.operation import Operation
from nmdc_runtime.resources.core import RuntimeApiSiteClient


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
    return nmdc_database
