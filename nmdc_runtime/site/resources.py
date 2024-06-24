from dataclasses import dataclass
import json
import os
from datetime import timedelta, datetime, timezone
from functools import lru_cache
from typing import Any, Dict, List, Optional, Union

import requests
import requests_cache
from requests.auth import HTTPBasicAuth
from dagster import (
    build_init_resource_context,
    resource,
    StringSource,
    InitResourceContext,
)
from fastjsonschema import JsonSchemaValueException
from frozendict import frozendict
from linkml_runtime.dumpers import json_dumper
from pydantic import BaseModel, AnyUrl
from pymongo import MongoClient, ReplaceOne, InsertOne
from toolz import get_in
from toolz import merge

from nmdc_runtime.api.core.util import expiry_dt_from_now, has_passed
from nmdc_runtime.api.models.object import DrsObject, AccessURL, DrsObjectIn
from nmdc_runtime.api.models.operation import ListOperationsResponse
from nmdc_runtime.api.models.util import ListRequest
from nmdc_runtime.site.normalization.gold import normalize_gold_id
from nmdc_runtime.util import unfreeze, nmdc_jsonschema_validator_noidpatterns
from nmdc_schema import nmdc


class RuntimeApiClient:
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.headers = {}
        self.token_response = None
        self.refresh_token_after = None

    def ensure_token(self):
        if self.refresh_token_after is None or has_passed(self.refresh_token_after):
            self.get_token()

    def get_token_request_body(self):
        raise NotImplementedError()

    def get_token(self):
        rv = requests.post(self.base_url + "/token", data=self.get_token_request_body())
        self.token_response = rv.json()
        if "access_token" not in self.token_response:
            raise Exception(f"Getting token failed: {self.token_response}")

        self.headers["Authorization"] = f'Bearer {self.token_response["access_token"]}'
        self.refresh_token_after = expiry_dt_from_now(
            **self.token_response["expires"]
        ) - timedelta(seconds=5)

    def request(self, method, url_path, params_or_json_data=None):
        self.ensure_token()
        kwargs = {"url": self.base_url + url_path, "headers": self.headers}
        if isinstance(params_or_json_data, BaseModel):
            params_or_json_data = params_or_json_data.model_dump(exclude_unset=True)
        if method.upper() == "GET":
            kwargs["params"] = params_or_json_data
        else:
            kwargs["json"] = params_or_json_data
        rv = requests.request(method, **kwargs)
        rv.raise_for_status()
        return rv


class RuntimeApiUserClient(RuntimeApiClient):
    def __init__(self, username: str, password: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.username = username
        self.password = password

    def get_token_request_body(self):
        return {
            "grant_type": "password",
            "username": self.username,
            "password": self.password,
        }

    def submit_metadata(self, database: nmdc.Database):
        body = json_dumper.to_dict(database)
        return self.request("POST", "/metadata/json:submit", body)

    def validate_metadata(self, database: nmdc.Database):
        body = json_dumper.to_dict(database)
        return self.request("POST", "/metadata/json:validate", body)

    def get_run_info(self, run_id: str):
        return self.request("GET", f"/runs/{run_id}")

    def get_biosamples_by_gold_biosample_id(self, gold_biosample_id: str):
        gold_biosample_id = normalize_gold_id(gold_biosample_id)
        response = self.request(
            "POST",
            f"/queries:run",
            {
                "find": "biosample_set",
                "filter": {
                    "gold_biosample_identifiers": {
                        "$elemMatch": {"$eq": gold_biosample_id}
                    }
                },
            },
        )
        response.raise_for_status()
        return response.json()["cursor"]["firstBatch"]

    def get_omics_processing_records_by_gold_project_id(self, gold_project_id: str):
        gold_project_id = normalize_gold_id(gold_project_id)
        response = self.request(
            "POST",
            f"/queries:run",
            {
                "find": "omics_processing_set",
                "filter": {
                    "gold_sequencing_project_identifiers": {
                        "$elemMatch": {"$eq": gold_project_id}
                    }
                },
            },
        )
        response.raise_for_status()
        return response.json()["cursor"]["firstBatch"]

    def get_biosamples_for_study(self, study_id: str):
        response = self.request(
            "POST",
            f"/queries:run",
            {
                "find": "biosample_set",
                "filter": {"part_of": {"$elemMatch": {"$eq": study_id}}},
            },
        )
        response.raise_for_status()
        return response.json()["cursor"]["firstBatch"]

    def get_omics_processing_by_name(self, name: str):
        response = self.request(
            "POST",
            f"/queries:run",
            {
                "find": "omics_processing_set",
                "filter": {"name": {"$regex": name, "$options": "i"}},
            },
        )
        response.raise_for_status()
        return response.json()["cursor"]["firstBatch"]


class RuntimeApiSiteClient(RuntimeApiClient):
    def __init__(
        self, site_id: str, client_id: str, client_secret: str, *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.site_id = site_id
        self.client_id = client_id
        self.client_secret = client_secret

    def get_token_request_body(self):
        return {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }

    def put_object_in_site(self, object_in):
        return self.request("POST", f"/sites/{self.site_id}:putObject", object_in)

    def get_site_object_link(self, access_method):
        return self.request(
            "POST", f"/sites/{self.site_id}:getObjectLink", access_method
        )

    def get_operation(self, op_id):
        return self.request("GET", f"/operations/{op_id}")

    def operation_is_done(self, op_id):
        op = self.get_operation(op_id).json()
        return op.get("done") is True

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

    def ensure_object_tag(self, object_id, tag_id):
        object_type_ids = [
            t["id"] for t in self.request("GET", f"/objects/{object_id}/types").json()
        ]
        if tag_id not in object_type_ids:
            return self.request(
                "PUT", f"/objects/{object_id}/types", object_type_ids + [tag_id]
            )

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
            if str(access.url).startswith(
                os.getenv("API_HOST_EXTERNAL")
            ) and self.base_url == os.getenv("API_HOST"):
                access.url = AnyUrl(
                    str(access.url).replace(
                        os.getenv("API_HOST_EXTERNAL"), os.getenv("API_HOST")
                    )
                )
        else:
            access = AccessURL(url=method.access_url.url)
        return requests.get(str(access.url))

    def list_jobs(self, list_request=None):
        if list_request is None:
            params = {}
        else:
            if "filter" in list_request and isinstance(list_request["filter"], dict):
                list_request["filter"] = json.dumps(list_request["filter"])
            params = ListRequest(**list_request)
        return self.request("GET", "/jobs", params)

    def claim_job(self, job_id):
        return self.request("POST", f"/jobs/{job_id}:claim")

    def mint_id(self, schema_class, how_many=1):
        body = {"schema_class": {"id": schema_class}, "how_many": how_many}
        print(body)
        return self.request("POST", "/pids/mint", body)


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


@resource(
    config_schema={
        "base_url": StringSource,
        "username": StringSource,
        "password": StringSource,
    }
)
def runtime_api_user_client_resource(context):
    return RuntimeApiUserClient(
        base_url=context.resource_config["base_url"],
        username=context.resource_config["username"],
        password=context.resource_config["password"],
    )


@lru_cache
def get_runtime_api_site_client(run_config: frozendict):
    resource_context = build_init_resource_context(
        config=unfreeze(
            get_in(
                ["resources", "runtime_api_site_client", "config"],
                run_config,
            )
        )
    )
    return runtime_api_site_client_resource(resource_context)


@dataclass
class BasicAuthClient:
    base_url: str
    username: str
    password: str

    def request(
        self, endpoint: str, method: str = "GET", **kwargs
    ) -> requests.Response:
        auth = HTTPBasicAuth(self.username, self.password)
        response = requests.request(
            method, self.base_url + endpoint, auth=auth, **kwargs
        )
        response.raise_for_status()
        return response.json()


@dataclass
class GoldApiClient(BasicAuthClient):
    def _normalize_id(self, id: str) -> str:
        """
        Translates a CURIE into LocalId form

        :param id: CURIE or LocalId
        :return: LocalId
        """
        return id.replace("gold:", "")

    def fetch_biosamples_by_study(self, study_id: str) -> List[Dict[str, Any]]:
        id = self._normalize_id(study_id)
        results = self.request("/biosamples", params={"studyGoldId": id})
        return results

    def fetch_projects_by_study(self, study_id: str) -> List[Dict[str, Any]]:
        id = self._normalize_id(study_id)
        results = self.request("/projects", params={"studyGoldId": id})
        return results

    def fetch_analysis_projects_by_study(self, id: str) -> List[Dict[str, Any]]:
        id = self._normalize_id(id)
        results = self.request("/analysis_projects", params={"studyGoldId": id})
        return results

    def fetch_study(self, id: str) -> Union[Dict[str, Any], None]:
        id = self._normalize_id(id)
        results = self.request("/studies", params={"studyGoldId": id})
        if not results:
            return None
        return results[0]


@resource(
    config_schema={
        "base_url": StringSource,
        "username": StringSource,
        "password": StringSource,
    }
)
def gold_api_client_resource(context: InitResourceContext):
    return GoldApiClient(
        base_url=context.resource_config["base_url"],
        username=context.resource_config["username"],
        password=context.resource_config["password"],
    )


@dataclass
class NmdcPortalApiClient:

    base_url: str
    refresh_token: str
    access_token: Optional[str] = None
    access_token_expires_at: Optional[datetime] = None

    def _request(self, method: str, endpoint: str, **kwargs):
        r"""
        Submits a request to the specified API endpoint;
        after refreshing the access token, if necessary.
        """
        if self.access_token is None or datetime.now() > self.access_token_expires_at:
            refresh_response = requests.post(
                f"{self.base_url}/auth/refresh",
                json={"refresh_token": self.refresh_token},
            )
            refresh_response.raise_for_status()
            refresh_body = refresh_response.json()
            self.access_token_expires_at = datetime.now() + timedelta(
                seconds=refresh_body["expires_in"]
            )
            self.access_token = refresh_body["access_token"]

        headers = kwargs.get("headers", {})
        headers["Authorization"] = f"Bearer {self.access_token}"
        return requests.request(
            method, f"{self.base_url}{endpoint}", **kwargs, headers=headers
        )

    def fetch_metadata_submission(self, id: str) -> Dict[str, Any]:
        response = self._request("GET", f"/api/metadata_submission/{id}")
        response.raise_for_status()
        return response.json()


@resource(
    config_schema={
        "base_url": StringSource,
        "refresh_token": StringSource,
    }
)
def nmdc_portal_api_client_resource(context: InitResourceContext):
    return NmdcPortalApiClient(
        base_url=context.resource_config["base_url"],
        refresh_token=context.resource_config["refresh_token"],
    )


@dataclass
class NeonApiClient:
    base_url: str
    api_token: str
    session = requests_cache.CachedSession("neon_cache")

    def request(self, url):
        response = self.session.get(url, headers={"X-API-Token": self.api_token})
        response.raise_for_status()
        return response.json()

    def fetch_product_by_id(self, product_id: str):
        return self.request(self.base_url + f"/products/{product_id}")


@resource(config_schema={"base_url": StringSource, "api_token": StringSource})
def neon_api_client_resource(context: InitResourceContext):
    return NeonApiClient(
        base_url=context.resource_config["base_url"],
        api_token=context.resource_config["api_token"],
    )


class MongoDB:
    def __init__(
        self,
        host: str,
        dbname: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
    ):
        self.client = MongoClient(
            host=host,
            username=username,
            password=password,
            directConnection=True,
        )
        self.db = self.client[dbname]

    def add_docs(self, docs, validate=True, replace=True):
        try:
            if validate:
                nmdc_jsonschema_validator_noidpatterns(docs)
            rv = {}
            for collection_name, docs in docs.items():
                rv[collection_name] = self.db[collection_name].bulk_write(
                    [
                        (
                            ReplaceOne({"id": d["id"]}, d, upsert=True)
                            if replace
                            else InsertOne(d)
                        )
                        for d in docs
                    ]
                )
                now = datetime.now(timezone.utc)
                self.db.txn_log.insert_many(
                    [
                        {
                            "tgt": {"id": d.get("id"), "c": collection_name},
                            "type": "upsert",
                            "ts": now,
                            # "dtl": {},
                        }
                        for d in docs
                    ]
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
        config=unfreeze(
            get_in(
                ["resources", "mongo", "config"],
                run_config,
            )
        )
    )
    return mongo_resource(resource_context)
