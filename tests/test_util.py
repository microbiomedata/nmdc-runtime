import json
from pathlib import Path

import pytest
import fastjsonschema
from fastjsonschema import JsonSchemaValueException, JsonSchemaException
from nmdc_schema.validate_nmdc_json import get_nmdc_schema
from pymongo import MongoClient

from nmdc_runtime.util import nmdc_jsonschema_validate
from nmdc_schema import validate_nmdc_json

# from nmdc_schema.validate_nmdc_json import jsonschema
from jsonschema import validate, ValidationError


REPO_ROOT = Path(__file__).parent.parent


def test_nmdc_jsonschema_validate():
    with open(REPO_ROOT.joinpath("metadata-translation/examples/study_test.json")) as f:
        study_test = json.load(f)
        try:
            _ = nmdc_jsonschema_validate(study_test)
        except JsonSchemaValueException as e:
            pytest.fail(str(e))


def test_mongo_validate():
    schema = get_nmdc_schema()
    # schema["bsonType"] = "object"
    # schema.pop("$id", None)
    # schema.pop("$schema", None)
    # schema["$defs"] = schema.pop("definitions")
    # schema = {"$jsonSchema": schema}
    # print(type(schema))
    # return
    client = MongoClient(
        host="localhost",
        port=27018,
        username="admin",
        password="root",
    )
    db = client["nmdc_etl_staging"]
    collection_name = "test.study_test"

    db.drop_collection(collection_name)
    # db.create_collection(collection_name, validator=schema)
    db.create_collection(collection_name)
    # db.command({"collMod": collection_name, "validator": schema})
    collection = db[collection_name]

    # print("db:", db.collection_names())
    with open(REPO_ROOT.joinpath("metadata-translation/examples/study_test.json")) as f:
        study_test = json.load(f)
        collection.insert_many(study_test["study_set"])
        # collection.insert_one({"foo": "bar"})
        # print(db.validate_collection(collection))


def test_iterate_collection():
    client = MongoClient(
        host="localhost",
        port=27018,
        username="admin",
        password="root",
    )
    db = client["nmdc_etl_staging"]
    collection_name = "test.study_test"
    collection = db[collection_name]
    # collection.insert_one({"id": "1234", "foo": "bar"})

    validate_schema = fastjsonschema.compile(get_nmdc_schema())
    try:
        for doc in collection.find({}, {"_id": 0}):
            print(len(doc))
            # validate_schema({"study_set": [doc]})
            validate({"study_set": [doc]}, get_nmdc_schema())
    except JsonSchemaException as je:
        print(str(je))
    except ValidationError as ve:
        print(ve.message)
        # print("failed")


if __name__ == "__main__":
    # test_mongo_validate()
    test_iterate_collection()
