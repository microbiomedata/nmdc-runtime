import gzip
import os
import subprocess
from io import BytesIO
from tempfile import TemporaryDirectory

import requests

from nmdc_runtime.util import nmdc_jsonschema


def main():
    print("starting nmdcdb mongoexport...")
    print("connected to database...")

    collection_names = set(nmdc_jsonschema["$defs"]["Database"]["properties"])
    print("retrieved relevant collection names...")
    print(sorted(collection_names))
    print(f"filtering {len(collection_names)} collections...")
    heavy_collection_names = {"functional_annotation_set", "genome_feature_set"}
    collection_names = {c for c in collection_names if c not in heavy_collection_names}
    print(f"filtered collections to {len(collection_names)}:")
    print(sorted(collection_names))

    URL_PREFIX = "https://drs.microbiomedata.org/ga4gh/drs/v1/objects/"

    # Use 2021-10 exports
    print("Getting mongoexport URLs...")
    mongoexports = {}
    drs_bundle = requests.get(URL_PREFIX + "sys086d541").json()
    for entry in drs_bundle["contents"]:
        entry_collection_name = entry["name"].split(".")[0]
        if entry_collection_name in collection_names:
            url = URL_PREFIX + entry["id"]
            drs_object = requests.get(url).json()
            jsonl_gz_url = drs_object["access_methods"][0]["access_url"]["url"]
            mongoexports[entry_collection_name] = jsonl_gz_url
    print("Got mongoexport URLs. Importing to mongo...")

    with TemporaryDirectory() as tmpdirname:
        for collection_name, jsonl_gz_url in mongoexports.items():
            f_gz = BytesIO(requests.get(jsonl_gz_url).content)
            with gzip.open(f_gz, "rb") as f:
                file_content = f.read().decode()
                file_path = f"{tmpdirname}/{collection_name}.jsonl"
                with open(file_path, "w") as f_jsonl:
                    f_jsonl.write(file_content)
                cmd = (
                    f"mongoimport --host \"{os.getenv('MONGO_HOST').replace('mongodb://', '')}\" "
                    f"-u \"{os.getenv('MONGO_USERNAME')}\" -p \"{os.getenv('MONGO_PASSWORD')}\" "
                    f"--authenticationDatabase admin "
                    f"-d \"{os.getenv('MONGO_DBNAME')}\" -c {collection_name} "
                    f"-f {file_path} --drop"
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
