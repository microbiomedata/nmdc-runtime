from nmdc_runtime.site import validation
from pathlib import Path

from dagster import op, Failure, AssetMaterialization
from dagster.core.definitions.events import AssetKey, Output
from dagster.core.definitions.output import Out
from fastjsonschema import JsonSchemaValueException
from jsonschema import Draft7Validator
from nmdc_schema.validate_nmdc_json import get_nmdc_schema
from toolz import dissoc

from nmdc_runtime.lib.nmdc_etl_class import NMDC_ETL
from nmdc_runtime.site.resources import mongo_resource
from nmdc_runtime.util import nmdc_jsonschema_validate


mode_prod = {"resource_defs": {"mongo": mongo_resource}}
mode_dev = {
    "resource_defs": {"mongo": mongo_resource}
}  # Connect to a real MongoDB instance for development.
mode_test = {
    "resource_defs": {"mongo": mongo_resource}
}  # Connect to a real MongoDB instance for testing.

config_prod = {
    "resources": {
        "mongo": {
            "config": {
                "host": {"env": "MONGO_HOST"},
                "username": {"env": "MONGO_USERNAME"},
                "password": {"env": "MONGO_PASSWORD"},
                "dbname": "nmdc_etl_staging",
            },
        }
    },
    "ops": {},
}

config_test = {
    "resources": {
        "mongo": {
            "config": {
                # local docker container via docker-compose.yml
                "host": "mongo",
                "username": "admin",
                "password": "root",
                "dbname": "nmdc_etl_staging",
            },
        }
    },
    "ops": {},
}

preset_prod = dict(**mode_prod, config=config_prod)
preset_test = dict(**mode_test, config=config_test)


@op()
def schema_validate(context, data: tuple):
    def schema_validate_asset(collection_name, status, errors):
        return AssetMaterialization(
            asset_key=AssetKey(["translation", f"{collection_name}_translation"]),
            description=f"{collection_name} translation validation",
            metadata={"status": status, "errors": errors},
        )

    collection_name, documents = data
    _, schema_collection_name = collection_name.split(".")
    try:
        nmdc_jsonschema_validate({schema_collection_name: documents})
        context.log.info(f"data for {collection_name} is valid")
        yield schema_validate_asset(collection_name, "valid", "none")
        # return data  # do I need a return statement and an Output?
    except JsonSchemaValueException as e:
        context.log.error("validation failed for {collection_name}" + str(e))
        yield schema_validate_asset(collection_name, "not valid", str(e))
        raise Failure(str(e))
    finally:
        yield Output(data)


# use this to set collection name via config
# @op(required_resource_keys={"mongo"}, config_schema={"collection_name": str})
# def validate_mongo_collection(context):

# calling op using collection name paramater
@op(required_resource_keys={"mongo"})
def validate_mongo_collection(context, collection_name: str):
    def schema_validate_asset(collection_name, status, errors):
        return AssetMaterialization(
            asset_key=AssetKey(["validation", f"{collection_name}_validation"]),
            description=f"{collection_name} translation validation",
            metadata={"status": status, "errors": str(errors)},
        )

    # use if passing in collection name via config
    # collection_name = context.solid_config["collection_name"]

    mongo_db = context.resources.mongo.db
    collection = mongo_db[collection_name]  # get mongo collection
    db_set = collection_name.split(".")[0]

    validator = Draft7Validator(get_nmdc_schema())
    validation_errors = []

    for count, doc in enumerate(collection.find()):
        # add logging for progress?
        # e.g.: if count % 1000 == 0: context.log.info(“done X of Y")
        try:
            doc = dissoc(doc, "_id")  # dissoc _id
            errors = list(validator.iter_errors({f"{db_set}": [doc]}))
            if len(errors) > 0:
                if "id" in doc.keys():
                    errors = {doc["id"]: [e.message for e in errors]}
                else:
                    errors = {f"missing id ({count})": [e.message for e in errors]}
                validation_errors.append(errors)
        except Exception as exception:
            print(str(exception))
            context.log.error("ERROR: " + str(exception))
            raise Failure(str(exception))
        finally:
            if len(validation_errors) > 0:
                schema_validate_asset(collection_name, "not valid", validation_errors)
                is_valid = False
            else:
                schema_validate_asset(collection_name, "valid", "none")
                is_valid = True
            Output(is_valid)
