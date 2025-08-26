import os

from functools import lru_cache
from pymongo.database import Database as MongoDatabase
from subprocess import Popen, PIPE, STDOUT, CalledProcessError
from refscan.lib.helpers import get_collection_names_from_schema
from toolz import dissoc

from nmdc_runtime.site.resources import mongo_resource
from nmdc_runtime.util import nmdc_schema_view


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


def run_and_log(shell_cmd, context):
    process = Popen(shell_cmd, shell=True, stdout=PIPE, stderr=STDOUT)
    for line in process.stdout:
        context.log.info(line.decode())
    retcode = process.wait()
    if retcode:
        raise CalledProcessError(retcode, process.args)


@lru_cache
def schema_collection_has_index_on_id(mdb: MongoDatabase) -> dict:
    """
    TODO: Document this function.
    """
    schema_view = nmdc_schema_view()
    present_collection_names = set(mdb.list_collection_names())
    return {
        name: (
            name in present_collection_names and "id_1" in mdb[name].index_information()
        )
        for name in get_collection_names_from_schema(schema_view)
    }


def get_basename(filename: str) -> str:
    return os.path.basename(filename)


def nmdc_study_id_to_filename(nmdc_study_id: str) -> str:
    return nmdc_study_id.replace(":", "_").replace("-", "_")


def get_instruments_by_id(mdb: MongoDatabase) -> dict[str, dict]:
    """Get all documents from the instrument_set collection in a dict keyed by id."""
    return {
        instrument["id"]: instrument for instrument in mdb["instrument_set"].find({})
    }


def mongo_add_docs_result_as_dict(rv):
    """TODO: Document this function."""
    return {
        collection_name: dissoc(bulk_write_result.bulk_api_result, "upserted")
        for collection_name, bulk_write_result in rv.items()
    }
