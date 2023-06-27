"""
Usage:
$ export $(grep -v '^#' .env | xargs)
$ nmdcdb-mongoexport
"""

import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import click
from pymongo.database import Database as MongoDatabase
from toolz import assoc

from nmdc_runtime.api.core.util import pick
from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.site.repository import run_config_frozen__normal_env
from nmdc_runtime.site.resources import get_mongo
from nmdc_runtime.util import nmdc_jsonschema, schema_collection_names_with_id_field


def collection_stats(mdb: MongoDatabase):
    """Stats for each collection, with a scale factor of 1024, so sizes are in KB."""
    names = [
        n
        for n in mdb.list_collection_names()
        if n.endswith("_set") and mdb[n].estimated_document_count() > 0
    ]
    out = []
    for n in names:
        stats = pick(
            [
                "size",
                "count",
                "avgObjSize",
                "scaleFactor",
            ],
            mdb.command({"collStats": n, "scale": 1024}),
        )
        out.append(assoc(stats, "collection", n))
    return out


@click.command()
@click.option("--just-schema-collections", is_flag=True, default=False)
def main(just_schema_collections):
    print("starting nmdcdb mongoexport...")
    mdb = get_mongo_db()
    print("connected to database...")

    if just_schema_collections:
        collection_names = set(mdb.list_collection_names()) & set(
            schema_collection_names_with_id_field()
        )
    else:
        collection_names = set(mdb.list_collection_names())
        collection_names -= {c for c in collection_names if c.startswith("system.")}

    print("retrieved relevant collection names...")
    print(sorted(collection_names))
    print(f"filtering {len(collection_names)} collections...")
    collection_names = {
        c for c in collection_names if mdb[c].estimated_document_count() > 0
    }
    print(f"filtered collections to {len(collection_names)}:")
    print(sorted(collection_names))

    now = (
        datetime.now(tz=ZoneInfo("America/Los_Angeles"))
        .isoformat(timespec="seconds")
        .replace(":", "")
    )
    print(f"ensuring ~/mongoexport/{now} directory for exports")
    out_dir = (
        Path("~/mongoexport")
        .expanduser()
        .joinpath(now)
        .joinpath(os.getenv("MONGO_DBNAME"))
    )
    os.makedirs(str(out_dir), exist_ok=True)

    n_colls = len(collection_names)
    heavy_collection_names = {"functional_annotation_set", "genome_feature_set"}
    for i, collname in enumerate(collection_names):
        filepath = out_dir.joinpath(collname + ".jsonl")
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


if __name__ == "__main__":
    main()
