"""
Usage:
$ export $(grep -v '^#' .env.localhost | xargs)
$ # import from a JSON-serialized nmdc:Database LinkML object dump.
$ nmdcdb-mongoimport nmdc:Database.json
"""

import json
import os
import sys
import subprocess
from tempfile import TemporaryDirectory

from nmdc_runtime.util import nmdc_jsonschema


def main():
    print("starting nmdcdb mongoimport...")

    collection_names = set(nmdc_jsonschema["$defs"]["Database"]["properties"])

    filepath = sys.argv[1]
    with open(filepath) as f:
        schema_db = json.load(f)  # try to load in memory

    with TemporaryDirectory() as tmpdirname:
        for coll_name, docs in schema_db.items():
            if coll_name not in collection_names:
                raise Exception(f"{coll_name=} not in {collection_names=}")

            file_path = f"{tmpdirname}/{coll_name}.jsonl"
            with open(file_path, "w") as f_jsonl:
                for i, d in enumerate(docs, start=1):
                    f_jsonl.write(json.dumps(d) + ("" if i == len(docs) else "\n"))
            cmd = (
                f"mongoimport --host \"{os.getenv('MONGO_HOST').replace('mongodb://', '')}\" "
                f"-u \"{os.getenv('MONGO_USERNAME')}\" -p \"{os.getenv('MONGO_PASSWORD')}\" "
                f"--authenticationDatabase admin "
                f"-d \"{os.getenv('MONGO_DBNAME')}\" -c {coll_name} "
                f"--file {file_path} --drop"
            )
            print(cmd.replace(f"-p \"{os.getenv('MONGO_PASSWORD')}\"", ""))
            subprocess.run(
                cmd,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                check=True,
            )


if __name__ == "__main__":
    main()
