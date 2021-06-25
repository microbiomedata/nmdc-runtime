from dagster import resource, StringSource, build_init_resource_context
from fastjsonschema import JsonSchemaValueException
from pymongo import MongoClient, ReplaceOne
from toolz import get_in

from nmdc_runtime.util import nmdc_jsonschema_validate


class MongoDB:
    def __init__(self, host: str, username: str, password: str, dbname: str):
        self.client = MongoClient(host=host, username=username, password=password)
        self.db = self.client[dbname]

    def add_docs(self, docs, validate=True):
        try:
            if validate:
                nmdc_jsonschema_validate(docs)
            for collection_name, docs in docs.items():
                self.db[collection_name].bulk_write(
                    [ReplaceOne({"id": d["id"]}, d, upsert=True) for d in docs]
                )
        except JsonSchemaValueException as e:
            raise ValueError(e.message)


@resource(
    config_schema={
        "host": StringSource,
        "username": StringSource,
        "password": StringSource,
        "dbname": StringSource,
    }
)
def mongo_resource(context):
    return MongoDB(
        host=context.resource_config["host"],
        username=context.resource_config["username"],
        password=context.resource_config["password"],
        dbname=context.resource_config["dbname"],
    )


def get_mongo(run_config: dict):
    resource_context = build_init_resource_context(
        config=get_in(
            ["resources", "mongo", "config"],
            run_config,
        )
    )
    return mongo_resource(resource_context)
