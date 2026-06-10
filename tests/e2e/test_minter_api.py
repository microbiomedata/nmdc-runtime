import pytest
import requests
from fastapi import status

from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.minter.config import schema_classes
from nmdc_runtime.site.resources import RuntimeApiSiteClient

schema_class = schema_classes()[0]["id"]


@pytest.fixture
def mint_one_id_response(api_site_client: RuntimeApiSiteClient):
    rv = api_site_client.mint_id(schema_class).json()

    yield rv

    # Delete minted ID if the fixture-using test did not delete it already.
    try:
        rv = api_site_client.request("GET", f"/pids/resolve/{rv[0]}")
    except requests.HTTPError as exc_info:
        if exc_info.response.status_code == 404:
            return

        assert (
            api_site_client.request(
                "POST",
                f"/pids/delete",
                {"id_name": rv[0]},
            ).status_code
            == 200
        )


def test_minter_api_mint(mint_one_id_response):
    rv = mint_one_id_response
    assert len(rv) == 1 and rv[0].startswith("nmdc:")


def test_minter_api_resolve(mint_one_id_response, api_site_client):
    id_name = mint_one_id_response[0]
    rv = api_site_client.request("GET", f"/pids/resolve/{id_name}").json()
    assert rv["id"] == id_name and rv["status"] == "draft"


def test_minter_api_bind(mint_one_id_response, api_site_client):
    id_name = mint_one_id_response[0]
    rv = api_site_client.request(
        "POST",
        f"/pids/bind",
        {"id_name": id_name, "metadata_record": {"foo": "bar"}},
    ).json()
    assert (
        rv["id"] == id_name
        and rv["status"] == "draft"
        and rv["bindings"] == {"foo": "bar"}
    )


def test_minter_api_delete(mint_one_id_response, api_site_client):
    id_name = mint_one_id_response[0]
    rv = api_site_client.request(
        "POST",
        f"/pids/delete",
        {"id_name": id_name},
    )
    assert rv.status_code == 200


def test_mint_id(api_site_client):
    """Confirm a site client can mint an ID."""

    number_of_ids = 1
    schema_class_uri = "nmdc:Study"
    body = {"schema_class": {"id": schema_class_uri}, "how_many": number_of_ids}
    response = api_site_client.request("POST", "/pids/mint", body)
    assert response.status_code == status.HTTP_200_OK
    response_body = response.json()
    assert isinstance(response_body, list)
    assert len(response_body) == number_of_ids


def test_mint_id_rejects_invalid_typecode(api_site_client):
    """Confirm a site client cannot mint an ID with an invalid schema class URI."""

    number_of_ids = 1
    schema_class_uri = "potato"
    body = {"schema_class": {"id": schema_class_uri}, "how_many": number_of_ids}

    with pytest.raises(requests.HTTPError) as exc_info:
        _ = api_site_client.request("POST", "/pids/mint", body)
    assert exc_info.value.response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


def test_mint_id_rejects_invalid_quantity(api_site_client):
    """Confirm a site client cannot mint an invalid quantity of IDs."""

    number_of_ids = -1
    schema_class_uri = "nmdc:Study"
    body = {"schema_class": {"id": schema_class_uri}, "how_many": number_of_ids}

    with pytest.raises(requests.HTTPError) as exc_info:
        _ = api_site_client.request("POST", "/pids/mint", body)
    assert exc_info.value.response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


class TestMintWorkflowExecutionId:
    """Tests targeting the `/pids/mint/workflow_execution_id` endpoint."""

    @staticmethod
    def _delete_minter_id_records(ids: list[str]) -> None:
        """Helper method that deletes the minter ID records having the specified `id` values."""
        if len(ids) > 0:
            get_mongo_db().get_collection("minter.id_records").delete_many(
                {"id": {"$in": ids}}
            )

    def test_given_no_existing_id_it_mints_both_base_and_suffixed_wfe_id(self, api_site_client: RuntimeApiSiteClient):
        """
        Confirm that, when the user does not specify an existing workflow execution ID,
        the minter mints an ID having a new base and a ".1" suffix.
        """

        db = get_mongo_db()
        minter_id_records = db.get_collection("minter.id_records")

        # Make a list of the pre-existing IDs so we can delete any _others_ when cleaning up.
        pre_existing_ids = list(minter_id_records.distinct("id"))
        new_ids_filter = {"id": {"$nin": pre_existing_ids}}  # `$nin` is effectively `NOT IN`

        try:
            # Submit a request to mint an ID.
            response = api_site_client.request(
                "POST",
                "/pids/mint/workflow_execution_id",
                {"schema_class": {"id": "nmdc:NomAnalysis"}}
            )
            assert response.status_code == status.HTTP_200_OK

            # Confirm the returned ID has the format we expect.
            minted_id = response.json()
            assert isinstance(minted_id, str)
            assert minted_id.startswith("nmdc:wfnom-")
            assert minted_id.endswith(".1")

            # Confirm the returned ID was actually minted.
            assert minter_id_records.count_documents({
                "id": minted_id,
                "name": minted_id,
                "typecode.id": "nmdc:NomAnalysis_typecode",
                "status": "draft",
            }) == 1

            # Also (given that we didn't specify an existing ID), confirm that the minter also minted the base ID.
            base_id = minted_id.rstrip(".1")
            assert minter_id_records.count_documents({
                "id": base_id,
                "name": base_id,
                "typecode.id": "nmdc:NomAnalysis_typecode",
                "status": "draft",
            }) == 1
        finally:
            minter_id_records.delete_many(new_ids_filter)

    def test_it_rejects_invalid_existing_ids(self, api_site_client: RuntimeApiSiteClient):
        """
        Confirm that, when the user specifies an ID that is invalid (e.g. it doesn't exist;
        or it's not compatible with the specified class; or it is compatible with the specified
        class, but that class is invalid for this endpoint), the endpoint rejects the request.
        """
        db = get_mongo_db()
        minter_id_records = db.get_collection("minter.id_records")
        workflow_execution_set = db.get_collection("workflow_execution_set")

        # Make a list of the pre-existing IDs so we can delete any _others_ when cleaning up.
        pre_existing_ids = list(minter_id_records.distinct("id"))
        new_ids_filter = {"id": {"$nin": pre_existing_ids}}  # `$nin` is effectively `NOT IN`

        try:
            # Confirm the IDs we'll be using really don't exist in the database.
            non_existent_ids = ["nmdc:wfnom-00-nonexistent.1"]
            assert minter_id_records.count_documents({"id": {"$in": non_existent_ids}}, limit=1) == 0
            assert workflow_execution_set.count_documents({"id": {"$in": non_existent_ids}}, limit=1) == 0

            # Test: Nonexistent ID.
            with pytest.raises(requests.HTTPError) as exc_info:
                _ = api_site_client.request(
                    "POST",
                    "/pids/mint/workflow_execution_id",
                    {
                        "schema_class": {"id": "nmdc:NomAnalysis"},
                        "existing_id": non_existent_ids[0],
                    },
                )
            response = exc_info.value.response
            assert response.status_code == status.HTTP_400_BAD_REQUEST

            # Prepare: Generate an ID for a class that's not a concrete subclass of `WorkflowExecution`.
            response = api_site_client.request(
                "POST",
                "/pids/mint",
                {
                    "schema_class": {"id": "nmdc:Study"},
                    "how_many": 1,
                },
            )
            study_id: str = response.json()[0]
            assert response.status_code == status.HTTP_200_OK and isinstance(study_id, str)

            # Test: ID exists but is not compatible with the specified class.
            with pytest.raises(requests.HTTPError) as exc_info:
                _ = api_site_client.request(
                    "POST",
                    "/pids/mint/workflow_execution_id",
                    {
                        "schema_class": {"id": "nmdc:NomAnalysis"},
                        "existing_id": study_id,
                    },
                )
            response = exc_info.value.response
            assert response.status_code == status.HTTP_400_BAD_REQUEST

            # Test: ID exists but is not compatible with the specified class.
            with pytest.raises(requests.HTTPError) as exc_info:
                _ = api_site_client.request(
                    "POST",
                    "/pids/mint/workflow_execution_id",
                    {
                        "schema_class": {"id": "nmdc:Study"},
                        "existing_id": study_id,
                    },
                )
            response = exc_info.value.response
            assert response.status_code == status.HTTP_400_BAD_REQUEST
        finally:
            minter_id_records.delete_many(new_ids_filter)

    def test_it_mints_id_having_same_base_as_existing_id(
        self,
        api_site_client,
    ):
        """
        Confirm that, when the user does specify an existing workflow execution ID,
        the minted ID uses the same base and the integer in its "dot integer" suffix
        is 1 greater than the previously-largest integer already claimed for that base.
        """

        db = get_mongo_db()
        minter_id_records = db.get_collection("minter.id_records")
        workflow_execution_set = db.get_collection("workflow_execution_set")

        # Make a list of the pre-existing IDs so we can delete any _others_ when cleaning up.
        pre_existing_ids = list(minter_id_records.distinct("id"))
        new_ids_filter = {"id": {"$nin": pre_existing_ids}}  # `$nin` is effectively `NOT IN`

        base_wfe_id = f"nmdc:wfnom-00-000001"
        seeded_wfe_ids = [f"{base_wfe_id}.1", f"{base_wfe_id}.3"]

        try:
            seeded_workflow_executions = [{"id": id_, "type": "nmdc:NomAnalysis"} for id_ in seeded_wfe_ids]
            workflow_execution_set.insert_many(seeded_workflow_executions)

            # Submit a request to mint an ID.
            response = api_site_client.request(
                "POST",
                "/pids/mint/workflow_execution_id",
                {
                    "schema_class": {"id": "nmdc:NomAnalysis"},
                    "existing_id": seeded_wfe_ids[0],
                },
            )
            assert response.status_code == status.HTTP_200_OK

            # Confirm the returned ID has the format we expect.
            minted_id = response.json()
            assert isinstance(minted_id, str)
            assert minted_id.startswith("nmdc:wfnom-")
            assert minted_id.endswith(".4")  # since ".3" was claimed and ".4" was not

            # Confirm the returned ID was actually minted.
            # Note: The minter appends `_typecode` to the `typecode.id` value.
            assert minter_id_records.count_documents({
                "id": minted_id,
                "name": minted_id,
                "typecode.id": "nmdc:NomAnalysis_typecode",
                "status": "draft",
            }) == 1
        finally:
            minter_id_records.delete_many(new_ids_filter)
            workflow_execution_set.delete_many({"id": {"$in": seeded_wfe_ids}})

    def test_it_rejects_non_workflow_execution_schema_class(
        self,
        api_site_client,
    ):
        """
        Confirm the API endpoint refuses to mint an ID for a schema class that isn't
        a concrete subclass of `WorkflowExecution`.
        """

        # Test: Not a subclass.
        with pytest.raises(requests.HTTPError) as exc_info:
            _ = api_site_client.request(
                "POST",
                "/pids/mint/workflow_execution_id",
                {"schema_class": {"id": "nmdc:Study"}}
            )
        response = exc_info.value.response
        assert response.status_code == status.HTTP_400_BAD_REQUEST

        # Test: Not a subclass and not concrete.
        with pytest.raises(requests.HTTPError) as exc_info:
            _ = api_site_client.request(
                "POST",
                "/pids/mint/workflow_execution_id",
                {"schema_class": {"id": "nmdc:WorkflowExecution"}}
            )
        response = exc_info.value.response
        assert response.status_code == status.HTTP_400_BAD_REQUEST

        # Test: Not concrete.
        with pytest.raises(requests.HTTPError) as exc_info:
            _ = api_site_client.request(
                "POST",
                "/pids/mint/workflow_execution_id",
                {"schema_class": {"id": "nmdc:AnnotatingWorkflow"}}
            )
        response = exc_info.value.response
        assert response.status_code == status.HTTP_400_BAD_REQUEST

        # Test: Not a class defined in the schema.
        with pytest.raises(requests.HTTPError) as exc_info:
            _ = api_site_client.request(
                "POST",
                "/pids/mint/workflow_execution_id",
                {"schema_class": {"id": "nmdc:NonExistentClass"}}
            )
        response = exc_info.value.response
        assert response.status_code == status.HTTP_400_BAD_REQUEST
