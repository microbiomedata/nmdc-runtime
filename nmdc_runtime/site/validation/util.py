from dagster import op, AssetMaterialization, AssetKey, EventMetadata
from jsonschema import Draft7Validator
from nmdc_runtime.util import get_nmdc_jsonschema_dict
from toolz import dissoc

from nmdc_runtime.site.resources import mongo_resource

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

# use this to set collection name via config
# @op(required_resource_keys={"mongo"}, config_schema={"collection_name": str})
# def validate_mongo_collection(context):


@op(required_resource_keys={"mongo"})
def validate_mongo_collection(context, collection_name: str):
    context.log.info(f"collection_name: {collection_name}")

    # use if passing in collection name via config
    # collection_name = context.op_config["collection_name"]

    mongo_db = context.resources.mongo.db
    collection = mongo_db[collection_name]  # get mongo collection
    db_set = collection_name.split(".")[0]

    validator = Draft7Validator(get_nmdc_jsonschema_dict())
    validation_errors = []

    for count, doc in enumerate(collection.find()):
        # add logging for progress?
        # e.g.: if count % 1000 == 0: context.log.info(“done X of Y")
        doc = dissoc(doc, "_id")  # dissoc _id
        errors = list(validator.iter_errors({f"{db_set}": [doc]}))
        if len(errors) > 0:
            if "id" in doc.keys():
                errors = {doc["id"]: [e.message for e in errors]}
            else:
                errors = {f"missing id ({count})": [e.message for e in errors]}
            validation_errors.append(errors)

    return {"collection_name": collection_name, "errors": validation_errors}


@op
def write_to_local_file(context):
    # from tmpfile ...
    pass


@op
def announce_validation_report(context, report, api_object):
    collection_name = report["collection_name"]
    return AssetMaterialization(
        asset_key=AssetKey(["validation", f"{collection_name}_validation"]),
        description=f"{collection_name} translation validation",
        metadata={
            # https://docs.dagster.io/_apidocs/solids#event-metadata
            # also .json, .md, .path, .url, .python_artifact, ...
            "n_errors": EventMetadata.int(len(report["errors"])),
            "object_id": EventMetadata.text(api_object["id"]),
        },
    )


# TODO asset_sensor for validation asset that sends an email.
