r"""
This module contains `pytest` fixture definitions that `pytest` will automatically
make available to all tests within this directory and its descendant directories.
Reference: https://docs.pytest.org/en/stable/reference/fixtures.html#conftest-py-sharing-fixtures-across-multiple-files
"""

import os

import pytest

from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.site.resources import RuntimeApiSiteClient, RuntimeApiUserClient
from tests.test_api.test_endpoints import ensure_test_resources


@pytest.fixture
def base_url() -> str:
    r"""Returns the base URL of the API."""

    base_url = os.getenv("API_HOST")
    assert isinstance(base_url, str), "Base URL is not defined"
    return base_url


@pytest.fixture
def api_site_client():
    mdb = get_mongo_db()
    rs = ensure_test_resources(mdb)
    return RuntimeApiSiteClient(base_url=os.getenv("API_HOST"), **rs["site_client"])


@pytest.fixture
def api_user_client():
    mdb = get_mongo_db()
    rs = ensure_test_resources(mdb)
    return RuntimeApiUserClient(base_url=os.getenv("API_HOST"), **rs["user"])
