import os
from functools import lru_cache
from subprocess import Popen, PIPE, STDOUT, CalledProcessError

from pymongo.database import Database as MongoDatabase

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


def run_and_log(shell_cmd, context):
    process = Popen(shell_cmd, shell=True, stdout=PIPE, stderr=STDOUT)
    for line in process.stdout:
        context.log.info(line.decode())
    retcode = process.wait()
    if retcode:
        raise CalledProcessError(retcode, process.args)


@lru_cache
def collection_indexed_on_id(mdb: MongoDatabase) -> dict:
    set_collection_names = [
        name for name in mdb.list_collection_names() if name.endswith("_set")
    ]
    return {
        name: ("id_1" in mdb[name].index_information()) for name in set_collection_names
    }


def get_basename(filename: str) -> str:
    return os.path.basename(filename)
