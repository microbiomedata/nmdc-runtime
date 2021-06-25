from datetime import timedelta

from dagster import resource, StringSource, build_init_resource_context
import requests
from toolz import merge, get_in

from nmdc_runtime.api.core.util import expiry_dt_from_now, has_passed
from nmdc_runtime.api.models.object import DrsObject, AccessURL
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


def get_runtime_api_site_client(run_config: dict):
    resource_context = build_init_resource_context(
        config=get_in(
            ["resources", "runtime_api_site_client", "config"],
            run_config,
        )
    )
    return runtime_api_site_client_resource(resource_context)
