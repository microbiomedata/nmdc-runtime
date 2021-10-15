"""
Usage:
$ export $(grep -v '^#' .env | xargs)
$ nmdcdb-mongoexport
"""

import os
import subprocess
import warnings
from datetime import datetime, timezone
from pathlib import Path

import dagster

warnings.filterwarnings("ignore", category=dagster.ExperimentalWarning)

from nmdc_runtime.site.repository import run_config_frozen__normal_env
from nmdc_runtime.site.resources import get_mongo
from nmdc_runtime.util import nmdc_jsonschema


def main():
    print("starting nmdcdb mongoexport...")
    mongo = get_mongo(run_config_frozen__normal_env)
    mdb = mongo.db
    print("connected to database...")

    collection_names = set(mdb.list_collection_names()) & set(
        nmdc_jsonschema["$defs"]["Database"]["properties"]
    )
    print("retrieved relevant collection names...")
    print(sorted(collection_names))
    print(f"filtering {len(collection_names)} collections...")
    collection_names = {
        c for c in collection_names if mdb[c].estimated_document_count() > 0
    }
    print(f"filtered collections to {len(collection_names)}:")
    print(sorted(collection_names))

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    print(f"ensuring ~/mongoexport/{today} directory for exports")
    today_dir = Path("~/mongoexport").expanduser().joinpath(today)
    os.makedirs(str(today_dir), exist_ok=True)

    n_colls = len(collection_names)
    heavy_collection_names = {"functional_annotation_set", "genome_feature_set"}
    for i, collname in enumerate(collection_names):
        filepath = today_dir.joinpath(collname + ".jsonl")
        cmd = (
            f"mongoexport --host \"{os.getenv('MONGO_HOST').replace('mongodb://','')}\" "
            f"-u \"{os.getenv('MONGO_USERNAME')}\" -p \"{os.getenv('MONGO_PASSWORD')}\" "
            f"--authenticationDatabase admin "
            f"-d \"{os.getenv('MONGO_DBNAME')}\" -c {collname} "
            f"-o {filepath} "
        )
        print(cmd.replace(f"-p \"{os.getenv('MONGO_PASSWORD')}\"", ""))
        if collname not in heavy_collection_names:
            tic = datetime.now(timezone.utc)
            print(f"[{i + 1}/{n_colls}] {collname} export started at {tic.isoformat()}")
            subprocess.run(
                cmd,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                check=True,
            )
            print("compressing...")
            subprocess.run(
                f"gzip {filepath}",
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                check=True,
            )
            toc = datetime.now(timezone.utc)
            print(
                f"[{i + 1}/{n_colls}] {collname} export finished at {toc.isoformat()}"
            )
            print(f"[{i + 1}/{n_colls}] {collname} export took {toc - tic}")
        else:
            print(f"skipping {collname}. You should run the above command manually.")
