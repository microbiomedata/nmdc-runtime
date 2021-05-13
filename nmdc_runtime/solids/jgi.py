import json
import os
from subprocess import Popen, PIPE, STDOUT, CalledProcessError
from zipfile import ZipFile

from dagster import (
    solid,
    AssetMaterialization,
    Output,
    EventMetadata,
    Failure,
    OutputDefinition,
)


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
        asset_key="metadata-translation/nmdc_merged_data.tsv.zip",
        description="input to metadata-translation run_etl",
        metadata={
            "path": EventMetadata.path(storage_path),
        },
    )
    yield Output(storage_path)


@solid(
    output_defs=[OutputDefinition(dict, description="a NMDC-Schema 'Database' object")]
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
        asset_key="metadata-translation/nnmdc_database.json.zip",
        description="output of metadata-translation run_etl",
        metadata={
            "path": EventMetadata.path(storage_path),
        },
    )
    yield Output(rv)
