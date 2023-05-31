from pathlib import Path

from dagster import op, Failure, AssetMaterialization
from dagster.core.definitions.events import AssetKey, Output
from fastjsonschema import JsonSchemaValueException

from nmdc_runtime.lib.nmdc_etl_class import NMDC_ETL
from nmdc_runtime.site.resources import mongo_resource
from nmdc_runtime.util import nmdc_jsonschema_validator

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
    "ops": {
        "load_nmdc_etl_class": {
            "config": {
                "data_file": str(
                    Path(__file__).parent.parent.parent.parent.joinpath(
                        "metadata-translation/src/data/nmdc_merged_data.tsv.zip"
                    )
                ),
                "sssom_map_file": "",
                "spec_file": str(
                    Path(__file__).parent.parent.parent.parent.joinpath(
                        "nmdc_runtime/lib/nmdc_data_source.yaml"
                    )
                ),
            }
        }
    },
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
    "ops": {
        "load_nmdc_etl_class": {
            "config": {
                "data_file": str(
                    Path(__file__).parent.parent.parent.parent.joinpath(
                        "metadata-translation/src/data/nmdc_merged_data.tsv.zip"
                    )
                ),
                "sssom_map_file": "",
                "spec_file": str(
                    Path(__file__).parent.parent.parent.parent.joinpath(
                        "nmdc_runtime/lib/nmdc_data_source.yaml"
                    )
                ),
            }
        }
    },
}

preset_prod = dict(**mode_prod, config=config_prod)
preset_test = dict(**mode_test, config=config_test)


@op
def load_nmdc_etl_class(context) -> NMDC_ETL:
    # build instance of NMDC_ETL class
    etl = NMDC_ETL(
        merged_data_file=context.op_config["data_file"],
        data_source_spec_file=context.op_config["spec_file"],
        sssom_file="",
    )
    return etl


@op(required_resource_keys={"mongo"})
def load_mongo_collection(context, data: tuple):
    mongo_db = context.resources.mongo.db
    collection_name, documents = data
    collection = mongo_db[collection_name]  # get mongo collection

    # drop collection if exists
    collection.drop()

    # insert data
    collection.insert(documents)
    context.log.info(f"inserted {len(documents)} documents into {collection.name}")
    return collection_name


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
        nmdc_jsonschema_validator({schema_collection_name: documents})
        context.log.info(f"data for {collection_name} is valid")
        yield schema_validate_asset(collection_name, "valid", "none")
        return data  # do I need a return statement and an Output?
    except JsonSchemaValueException as e:
        context.log.error(f"validation failed for {schema_collection_name} " + str(e))
        context.log.error(f"documents: {documents}")
        yield schema_validate_asset(collection_name, "not valid", str(e))
        raise Failure(str(e))
    finally:
        yield Output(data)
