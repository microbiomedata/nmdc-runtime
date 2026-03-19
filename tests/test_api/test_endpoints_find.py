import csv
import io

import pytest
import requests
from fastapi import status

from nmdc_runtime.api.db.mongo import get_mongo_db
from tests.lib.faker import Faker


class TestGetAdminDataObjectURLs:
    http_method = "GET"
    url_path = "/admin/data_object_urls"

    @pytest.fixture
    def db_having_data_objects_outputted_by_wfes(self):
        mdb = get_mongo_db()
        workflow_execution_set = mdb.workflow_execution_set
        data_object_set = mdb.data_object_set

        # Insert some `DataObject`s having distinct URLs; and a `WorkflowExecution` having those
        # `DataObject`s as its output.
        faker = Faker()
        data_objects = faker.generate_data_objects(3, name="Name")
        data_object_ids = [data_object["id"] for data_object in data_objects]
        data_objects[0]["url"] = "https://data.microbiomedata.org/data/1.txt"
        data_objects[1]["url"] = "https://nmdcdemo.emsl.pnnl.gov/data/2.txt"
        workflow_executions = faker.generate_metagenome_annotations(
            1,
            was_informed_by=["nmdc:dgns-00-000001"],
            has_input=["nmdc:bsm-00-000001"],
            has_output=data_object_ids,
        )
        workflow_execution_ids = [wfe["id"] for wfe in workflow_executions]
        assert workflow_execution_set.count_documents({"id": {"$in": workflow_execution_ids}}) == 0
        assert data_object_set.count_documents({"id": {"$in": data_object_ids}}) == 0
        workflow_execution_set.insert_many(workflow_executions)
        data_object_set.insert_many(data_objects)

        yield mdb

        # Clean up: Delete the documents we inserted earlier.
        workflow_execution_set.delete_many({"id": {"$in": workflow_execution_ids}})
        data_object_set.delete_many({"id": {"$in": data_object_ids}})

    def test_it_returns_403_for_non_admin_user(self, api_user_client):
        with pytest.raises(requests.exceptions.HTTPError) as exc_info:
            api_user_client.request(self.http_method, self.url_path)
        assert exc_info.value.response.status_code == status.HTTP_403_FORBIDDEN

    def test_it_returns_204_when_no_urls_begin_with_prefix(
            self, api_admin_user_client, db_having_data_objects_outputted_by_wfes
    ):
        response = api_admin_user_client.request(
            self.http_method, f"{self.url_path}?prefix=https://example.com/no-matching-urls",
        )
        assert response.status_code == status.HTTP_204_NO_CONTENT

    def test_it_returns_only_urls_that_begin_with_prefix(
            self, api_admin_user_client, db_having_data_objects_outputted_by_wfes
    ):
        response = api_admin_user_client.request(
            self.http_method, f"{self.url_path}?prefix=https://data.microbiomedata.org",
        )
        assert response.status_code == status.HTTP_200_OK

        # Confirm the TSV report contains the URLs we expect.
        tsv_content = response.text
        reader = csv.reader(io.StringIO(tsv_content), delimiter="\t")
        received_urls = [row[0] for row in reader]
        assert received_urls == ["https://data.microbiomedata.org/data/1.txt"]

    def test_it_returns_urls_of_wfe_outputted_data_objects(
        self, api_admin_user_client, db_having_data_objects_outputted_by_wfes
    ):
        # Send a request to the API endpoint.
        response = api_admin_user_client.request(self.http_method, f"{self.url_path}")
        assert response.status_code == status.HTTP_200_OK

        # Confirm the TSV report contains the URLs we expect.
        tsv_content = response.text
        reader = csv.reader(io.StringIO(tsv_content), delimiter="\t")
        received_urls = [row[0] for row in reader]
        assert len(received_urls) == 2
        assert set(received_urls) == set([
            "https://data.microbiomedata.org/data/1.txt",
            "https://nmdcdemo.emsl.pnnl.gov/data/2.txt"
        ])
