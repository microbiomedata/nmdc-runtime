import os
import tarfile
from copy import deepcopy

import json
import sys
from pathlib import Path

import fastjsonschema
import pytest
from fastjsonschema import JsonSchemaValueException, JsonSchemaException

from jsonschema import ValidationError, Draft7Validator
from nmdc_runtime.util import get_nmdc_jsonschema_dict
from pymongo.database import Database as MongoDatabase
from pymongo.write_concern import WriteConcern
import requests

from nmdc_runtime.site.repository import run_config_frozen__normal_env
from nmdc_runtime.site.resources import get_mongo

REPO_ROOT = Path(__file__).parent.parent


def without_id_patterns(nmdc_jsonschema):
    rv = deepcopy(nmdc_jsonschema)
    for cls_, spec in rv["$defs"].items():
        if "properties" in spec:
            if "id" in spec["properties"]:
                spec["properties"]["id"].pop("pattern", None)
    return rv


nmdc_jsonschema_validator = fastjsonschema.compile(
    without_id_patterns(get_nmdc_jsonschema_dict())
)


def test_nmdc_jsonschema_using_new_id_scheme():
    r"""
    Note: Until commit #9d6963567ea203b724f372b9b6ac612d6dd15bf2, this test was being
          skipped. When un-skipped, the test failed with the following error message
          (here, unrelated dictionary items have been replaced with "..."):
          ```
          Failed: ChemicalEntity.id: {..., 'pattern': '^[a-zA-Z0-9][a-zA-Z0-9_\\.]+:[a-zA-Z0-9_][a-zA-Z0-9_\\-\\/\\.,]*$', ...}
          ```
          In order to get the test to pass, the developer un-skipping the test
          did two things: (a) extracted the original argument from the `.startswith()`
          call into a `valid_prefix_patterns` tuple so we would check for multiple
          prefixes; and (b) added the prefix shown in the above error message
          (after replacing `\\.` with `\.`) to that tuple.
    """

    # nmdc_database_collection_instance_class_names
    for class_name, defn in get_nmdc_jsonschema_dict()["$defs"].items():
        if "properties" in defn and "id" in defn["properties"]:
            if "pattern" in defn["properties"]["id"]:
                valid_prefix_patterns: tuple = (
                    r"^(nmdc):",
                    r"^[a-zA-Z0-9][a-zA-Z0-9_\.]+:",
                )
                if not defn["properties"]["id"]["pattern"].startswith(valid_prefix_patterns):
                    pytest.fail(f"{class_name}.id: {defn['properties']['id']}")


def test_iterate_collection():
    mongo = get_mongo(run_config_frozen__normal_env)
    db = mongo.client["nmdc_etl_staging"]
    collection_name = "test.study_test"
    collection = db[collection_name]

    # collection.insert_one({"id": "1234", "foo": "bar", "baz": "buzz", "name": 5})
    validate_schema = fastjsonschema.compile(get_nmdc_jsonschema_dict())
    try:
        for count, doc in enumerate(collection.find({}, {"_id": 0})):
            print(len(doc))
            validate_schema({"study_set": [doc]})
            # validate({"study_set": [doc]}, get_nmdc_jsonschema_dict())
    except JsonSchemaException as je:
        print(str(je))
        # assert False, str(je)
    except ValidationError as ve:
        # print(ve.message)
        assert False, ve.message
    except Exception as e:
        # print(e)
        assert False, str(e)


def test_multiple_errors():
    mongo = get_mongo(run_config_frozen__normal_env)
    db = mongo.client["nmdc_etl_staging"]
    collection_name = "test.study_test"
    collection = db[collection_name]

    # collection.insert_one({"id": "1234", "foo": "bar", "baz": "buzz", "name": 5})
    # validate_schema = fastjsonschema.compile(get_nmdc_jsonschema_dict())
    validator = Draft7Validator(get_nmdc_jsonschema_dict())
    validation_errors = []

    for doc in collection.find({}, {"_id": 0}):
        print(len(doc))
        try:
            # fastjsonschema doesn't handle multiple errors!
            # validate_schema({"study_set": [doc]})
            errors = list(validator.iter_errors({"study_set": [doc]}))
            if len(errors) > 0:
                errors = {doc["id"]: [e.message for e in errors]}
                validation_errors.append(errors)
        except Exception as exception:
            print(str(exception))

    print(validation_errors)


def download_to(url, path):
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(path, "wb") as file:
            chunk_size = 8192
            print(f"Downloading file using stream {chunk_size=}")
            for chunk in response.iter_content(chunk_size=chunk_size):
                file.write(chunk)
        print(f"Downloaded {url} to {path}")
    else:
        print(f"Failed to download {url}. Status code: {response.status_code}")


def download_and_extract_tar(url, extract_to="."):
    # Download the file
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        tar_path = os.path.join(extract_to, "downloaded_file.tar")
        os.makedirs(extract_to, exist_ok=True)
        with open(tar_path, "wb") as file:
            chunk_size = 8192
            print(f"Downloading tar file using stream {chunk_size=}")
            for chunk in response.iter_content(chunk_size=chunk_size):
                file.write(chunk)
        print(f"Downloaded tar file to {tar_path}")

        # Extract the tar file
        with tarfile.open(tar_path, "r") as tar:
            tar.extractall(path=extract_to)
            print(f"Extracted tar file to {extract_to}")

        # Optionally, remove the tar file after extraction
        os.remove(tar_path)
        print(f"Removed tar file {tar_path}")
    else:
        print(f"Failed to download file. Status code: {response.status_code}")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        eval(f"{sys.argv[1]}()")
    else:
        # test_iterate_collection()
        test_multiple_errors()
