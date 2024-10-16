import os
from functools import lru_cache
from subprocess import Popen, PIPE, STDOUT, CalledProcessError

from pymongo.database import Database as MongoDatabase

from linkml_runtime.utils.schemaview import SchemaView
from nmdc_schema.get_nmdc_view import ViewGetter
from nmdc_runtime.api.db.mongo import get_collection_names_from_schema
from nmdc_runtime.minter.config import typecodes
from nmdc_runtime.site.resources import mongo_resource

mode_test = {
    "resource_defs": {"mongo": mongo_resource}
}  # Connect to a real MongoDB instance for testing.

config_test = {
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
}

DATABASE_CLASS_NAME = "Database"


def run_and_log(shell_cmd, context):
    process = Popen(shell_cmd, shell=True, stdout=PIPE, stderr=STDOUT)
    for line in process.stdout:
        context.log.info(line.decode())
    retcode = process.wait()
    if retcode:
        raise CalledProcessError(retcode, process.args)


@lru_cache
def schema_collection_has_index_on_id(mdb: MongoDatabase) -> dict:
    present_collection_names = set(mdb.list_collection_names())
    return {
        name: (
            name in present_collection_names and "id_1" in mdb[name].index_information()
        )
        for name in get_collection_names_from_schema()
    }


def get_basename(filename: str) -> str:
    return os.path.basename(filename)


def get_name_of_class_objects_in_collection(
    schema_view: SchemaView, collection_name: str
) -> str:
    slot_definition = schema_view.induced_slot(collection_name, DATABASE_CLASS_NAME)
    return slot_definition.range


@lru_cache
def get_class_names_to_collection_names_map():
    vg = ViewGetter()
    schema_view = vg.get_view()

    collection_names = get_collection_names_from_schema()

    class_names_to_collection_names = {}
    for collection_name in collection_names:
        class_name = get_name_of_class_objects_in_collection(
            schema_view, collection_name
        )
        class_names_to_collection_names[class_name] = collection_name

    return class_names_to_collection_names


def get_collection_from_typecode(doc_id: str):
    typecode = doc_id.split(":")[1].split("-")[0]
    class_map_data = typecodes()

    class_map = {
        entry["name"]: entry["schema_class"].split(":")[1] for entry in class_map_data
    }
    class_name = class_map.get(typecode)
    if class_name:
        collection_dict = get_class_names_to_collection_names_map()
        collection_name = collection_dict.get(class_name)
        return collection_name

    return None
