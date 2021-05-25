from dagster import resource, StringSource
from pymongo import MongoClient


class MongoDB:
    def __init__(self, host: str, username: str, password: str, dbname: str):
        self.client = MongoClient(host=host, username=username, password=password)
        self.db = self.client[dbname]


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
