from datetime import timedelta

from dagster import resource, StringSource
import requests
from toolz import merge

from nmdc_runtime.api.core.util import expiry_dt_from_now, has_passed
from nmdc_runtime.api.models.operation import ListOperationsResponse


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


@resource(
    config_schema={
        "base_url": StringSource,  # os.getenv("API_HOST")
        "site_id": StringSource,  # os.getenv("API_SITE_ID")
        "client_id": StringSource,  # os.getenv("API_SITE_CLIENT_ID")
        "client_secret": StringSource,  # os.getenv("API_SITE_CLIENT_SECRET")
    }
)
def runtime_api_site_client_resource(context):
    return RuntimeApiSiteClient(
        base_url=context.resource_config["host"],
        site_id=context.resource_config["username"],
        client_id=context.resource_config["password"],
        client_secret=context.resource_config["dbname"],
    )
