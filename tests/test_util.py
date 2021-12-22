import json
import sys
from pathlib import Path

import fastjsonschema
import pytest
from fastjsonschema import JsonSchemaValueException, JsonSchemaException

# from nmdc_schema.validate_nmdc_json import jsonschema
from jsonschema import ValidationError, Draft7Validator
from nmdc_schema.nmdc_data import get_nmdc_jsonschema_dict

from nmdc_runtime.site.repository import run_config_frozen__normal_env
from nmdc_runtime.site.resources import get_mongo
from nmdc_runtime.util import nmdc_jsonschema_validate

REPO_ROOT = Path(__file__).parent.parent


def test_nmdc_jsonschema_validate():
    with open(REPO_ROOT.joinpath("metadata-translation/examples/study_test.json")) as f:
        study_test = json.load(f)
        try:
            _ = nmdc_jsonschema_validate(study_test)
        except JsonSchemaValueException as e:
            pytest.fail(str(e))


def test_mongo_validate():
    schema = get_nmdc_jsonschema_dict()
    # schema["bsonType"] = "object"
    # schema.pop("$id", None)
    # schema.pop("$schema", None)
    # schema["$defs"] = schema.pop("definitions")
    # schema = {"$jsonSchema": schema}
    # print(type(schema))
    # return
    mongo = get_mongo(run_config_frozen__normal_env)
    db = mongo.client["nmdc_etl_staging"]
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


if __name__ == "__main__":
    if len(sys.argv) > 1:
        eval(f"{sys.argv[1]}()")
    else:
        # test_mongo_validate()
        # test_iterate_collection()
        test_multiple_errors()
