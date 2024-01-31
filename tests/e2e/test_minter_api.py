import os
import pytest

from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.minter.config import schema_classes
from nmdc_runtime.site.resources import RuntimeApiSiteClient
from tests.test_api.test_endpoints import ensure_test_resources

schema_class = schema_classes()[0]


def _get_client():
    mdb = get_mongo_db()
    rs = ensure_test_resources(mdb)
    return RuntimeApiSiteClient(base_url=os.getenv("API_HOST"), **rs["site_client"])


@pytest.mark.xfail(reason="Expect 422 Client Error: Unprocessable Entity for url: http://fastapi:8000/pids/mint")
def test_minter_api_mint():
    client = _get_client()
    rv = client.request(
        "POST", "/pids/mint", {"schema_class": schema_class, "how_many": 1}
    ).json()
    assert len(rv) == 1 and rv[0].startswith("nmdc:")


@pytest.mark.xfail(reason="Expect 422 Client Error: Unprocessable Entity for url: http://fastapi:8000/pids/mint")
def test_minter_api_resolve():
    client = _get_client()
    [id_name] = client.request(
        "POST", "/pids/mint", {"schema_class": schema_class, "how_many": 1}
    ).json()
    rv = client.request("GET", f"/pids/resolve/{id_name}").json()
    assert rv["id"] == id_name and rv["status"] == "draft"


@pytest.mark.xfail(reason="Expect 422 Client Error: Unprocessable Entity for url: http://fastapi:8000/pids/mint")
def test_minter_api_bind():
    client = _get_client()
    [id_name] = client.request(
        "POST", "/pids/mint", {"schema_class": schema_class, "how_many": 1}
    ).json()
    rv = client.request(
        "POST",
        f"/pids/bind",
        {"id_name": id_name, "metadata_record": {"foo": "bar"}},
    ).json()
    assert (
        rv["id"] == id_name
        and rv["status"] == "draft"
        and rv["bindings"] == {"foo": "bar"}
    )


@pytest.mark.xfail(reason="Expect 422 Client Error: Unprocessable Entity for url: http://fastapi:8000/pids/mint")

def test_minter_api_delete():
    client = _get_client()
    [id_name] = client.request(
        "POST", "/pids/mint", {"schema_class": schema_class, "how_many": 1}
    ).json()
    rv = client.request(
        "POST",
        f"/pids/delete",
        {"id_name": id_name},
    )
    assert rv.status_code == 200
