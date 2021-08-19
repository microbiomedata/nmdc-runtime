from datetime import timedelta
from functools import lru_cache
from typing import Optional

import requests
from dagster import build_init_resource_context
from dagster import resource, StringSource
from fastjsonschema import JsonSchemaValueException
from frozendict import frozendict
from pymongo import MongoClient, ReplaceOne
from terminusdb_client import WOQLClient
from toolz import get_in
from toolz import merge

from nmdc_runtime.api.core.util import expiry_dt_from_now, has_passed
from nmdc_runtime.api.models.object import DrsObject, AccessURL, DrsObjectIn
from nmdc_runtime.api.models.operation import ListOperationsResponse
from nmdc_runtime.util import nmdc_jsonschema_validate


class RuntimeApiSiteClient:
    def __init__(self, base_url: str, site_id: str, client_id: str, client_secret: str):
        self.base_url = base_url
        self.site_id = site_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.headers = {}
        self.token_response = None
        self.refresh_token_after = None
        self.get_token()

    def request(self, method, url_path, params_or_json_data=None):
        self.ensure_token()
        kwargs = {"url": self.base_url + url_path, "headers": self.headers}
        if method.upper() == "GET":
            kwargs["params"] = params_or_json_data
        else:
            kwargs["json"] = params_or_json_data
        return requests.request(method, **kwargs)

    def get_token(self):
        rv = requests.post(
            self.base_url + "/token",
            data={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            },
        )
        self.token_response = rv.json()
        if "access_token" not in self.token_response:
            raise Exception(f"Getting token failed: {self.token_response}")

        self.headers["Authorization"] = f'Bearer {self.token_response["access_token"]}'
        self.refresh_token_after = expiry_dt_from_now(
            **self.token_response["expires"]
        ) - timedelta(seconds=5)

    def ensure_token(self):
        if has_passed(self.refresh_token_after):
            self.get_token()

    def put_object_in_site(self, object_in):
        return self.request("POST", f"/sites/{self.site_id}:putObject", object_in)

    def get_site_object_link(self, access_method):
        return self.request(
            "POST", f"/sites/{self.site_id}:getObjectLink", access_method
        )

    def update_operation(self, op_id, op_patch):
        return self.request("PATCH", f"/operations/{op_id}", op_patch)

    def list_operations(self, req):
        rv = self.request("GET", "/operations", req)
        lor = ListOperationsResponse(**rv.json())
        resources_so_far = lor.resources
        if not lor.next_page_token:
            return resources_so_far
        else:
            resources_rest = self.list_operations(
                merge(req, {"page_token": lor.next_page_token})
            )
            return resources_so_far + resources_rest

    def create_object(self, drs_object_in: DrsObjectIn):
        DrsObjectIn(**drs_object_in)  # validate before network request
        return self.request("POST", "/objects", drs_object_in)

    def create_object_from_op(self, op_doc):
        return self.request("POST", "/objects", op_doc["result"])

    def get_object_info(self, object_id):
        return self.request("GET", f"/objects/{object_id}")

    def get_object_access(self, object_id, access_id):
        return self.request("GET", f"/objects/{object_id}/access/{access_id}")

    def get_object_bytes(self, object_id) -> requests.Response:
        obj = DrsObject(**self.get_object_info(object_id).json())
        method = obj.access_methods[0]
        if method.access_url is None:
            access = AccessURL(
                **self.get_object_access(object_id, method.access_id).json()
            )
        else:
            access = AccessURL(url=method.access_url)
        return requests.get(access.url)

    def claim_job(self, job_id):
        return self.request("POST", f"/jobs/{job_id}:claim")


@resource(
    config_schema={
        "base_url": StringSource,
        "site_id": StringSource,
        "client_id": StringSource,
        "client_secret": StringSource,
    }
)
def runtime_api_site_client_resource(context):
    return RuntimeApiSiteClient(
        base_url=context.resource_config["base_url"],
        site_id=context.resource_config["site_id"],
        client_id=context.resource_config["client_id"],
        client_secret=context.resource_config["client_secret"],
    )


@lru_cache
def get_runtime_api_site_client(run_config: frozendict):
    resource_context = build_init_resource_context(
        config=get_in(
            ["resources", "runtime_api_site_client", "config"],
            run_config,
        )
    )
    return runtime_api_site_client_resource(resource_context)


class MongoDB:
    def __init__(
        self,
        host: str,
        dbname: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
    ):
        self.client = MongoClient(host=host, username=username, password=password)
        self.db = self.client[dbname]

    def add_docs(self, docs, validate=True):
        try:
            if validate:
                nmdc_jsonschema_validate(docs)
            rv = {}
            for collection_name, docs in docs.items():
                rv[collection_name] = self.db[collection_name].bulk_write(
                    [ReplaceOne({"id": d["id"]}, d, upsert=True) for d in docs]
                )
            return rv
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
        dbname=context.resource_config["dbname"],
        username=context.resource_config["username"],
        password=context.resource_config["password"],
    )


@resource(
    config_schema={
        "host": StringSource,
        "dbname": StringSource,
    }
)
def mongo_insecure_test_resource(context):
    return MongoDB(
        host=context.resource_config["host"],
        dbname=context.resource_config["dbname"],
    )


@lru_cache
def get_mongo(run_config: frozendict):
    resource_context = build_init_resource_context(
        config=get_in(
            ["resources", "mongo", "config"],
            run_config,
        )
    )
    return mongo_resource(resource_context)


class TerminusDB:
    def __init__(self, server_url, user, key, account, dbid):
        self.client = WOQLClient(server_url=server_url)
        self.client.connect(user=user, key=key, account=account)
        db_info = self.client.get_database(dbid=dbid, account=account)
        if db_info is None:
            self.client.create_database(dbid=dbid, accountid=account, label=dbid)
            self.client.create_graph(graph_type="inference", graph_id="main")
        self.client.connect(user=user, key=key, account=account, db=dbid)


@resource(
    config_schema={
        "server_url": StringSource,
        "user": StringSource,
        "key": StringSource,
        "account": StringSource,
        "dbid": StringSource,
    }
)
def terminus_resource(context):
    return TerminusDB(
        server_url=context.resource_config["server_url"],
        user=context.resource_config["user"],
        key=context.resource_config["key"],
        account=context.resource_config["account"],
        dbid=context.resource_config["dbid"],
    )
