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
    print("starting nmdcdb mongodump...")
    mongo = get_mongo(run_config_frozen__normal_env)
    mdb = mongo.db
    print("connected to database...")

    collection_names = set(mdb.list_collection_names()) & set(
        nmdc_jsonschema["$defs"]["Database"]["properties"]
    )
    print("retrieved relevant collection names...")
    print(sorted(collection_names))
    print(f"filtering {len(collection_names)} collections...")
    heavy_collection_names = {"functional_annotation_set", "genome_feature_set"}
    collection_names = {c for c in collection_names if c not in heavy_collection_names}
    print(f"filtered collections to {len(collection_names)}:")
    print(sorted(collection_names))

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    print(f"ensuring ~/mongoexport/{today} directory for exports")
    today_dir = Path("~/nmdcdb-mongodump").expanduser().joinpath(today)
    os.makedirs(str(today_dir), exist_ok=True)

    collections_excluded = set(mdb.list_collection_names()) - collection_names
    collections_excluded_str = " ".join(
        ["--excludeCollection=" + c for c in collections_excluded]
    )

    filepath = today_dir.joinpath("nmdcdb.test.archive.gz")
    cmd = (
        f"mongodump --host \"{os.getenv('MONGO_HOST').replace('mongodb://','')}\" "
        f"-u \"{os.getenv('MONGO_USERNAME')}\" -p \"{os.getenv('MONGO_PASSWORD')}\" "
        f"--authenticationDatabase admin "
        f"-d \"{os.getenv('MONGO_DBNAME')}\" --gzip --archive={filepath} "
        f"{collections_excluded_str}"
    )
    print(cmd.replace(f"-p \"{os.getenv('MONGO_PASSWORD')}\"", ""))
    subprocess.run(
        cmd,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=True,
    )
