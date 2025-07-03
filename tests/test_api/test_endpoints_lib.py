import pytest

from fastapi.exceptions import HTTPException
from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.endpoints.lib.helpers import simulate_updates_and_check_references
from nmdc_runtime.api.models.query import UpdateCommand, UpdateStatement
from starlette import status
from tests.lib.faker import Faker


@pytest.fixture(scope="module")
def seeded_db():
    r"""Pytest fixture that yields a seeded database."""

    faker = Faker()
    study_a, study_b = faker.generate_studies(2)
    study_a["name"] = "Study A"
    study_b["name"] = "Study B"
    study_b["part_of"] = study_a["id"]
    
    mdb = get_mongo_db()
    assert mdb["study_set"].count_documents({"id": {"$in": [study_a["id"], study_b["id"]]}}) == 0
    mdb["study_set"].insert_many([study_a, study_b])
    
    yield mdb
    
    # 🧹 Clean up.
    mdb["study_set"].delete_many({"id": {"$in": [study_a["id"], study_b["id"]]}})


class TestSimulateUpdatesAndCheckReferences:
    r"""
    TODO: Add tests covering the multiple other scenarios that could arise.
    """
    def test_it_returns_none_when_operation_is_ok(self, seeded_db):
        assert simulate_updates_and_check_references(
            db=seeded_db,
            update_cmd=UpdateCommand(
                update="study_set",
                updates=[
                    UpdateStatement(q={"name": "Study A"}, u={"$set": {"name": "Study Alpha"}}),
                ],
            ),
        ) is None

    def test_it_aborts_when_introducing_a_broken_outgoing_reference_to_same_collection(self, seeded_db):
        assert seeded_db["study_set"].count_documents({"id": "nmdc:sty-00-000099"}) == 0
        with pytest.raises(HTTPException) as exc_info:
            simulate_updates_and_check_references(
                db=seeded_db,
                update_cmd=UpdateCommand(
                    update="study_set",
                    updates=[
                        UpdateStatement(q={"name": "Study A"}, u={"$set": {"part_of": "nmdc:sty-00-000099"}}),
                    ],
                ),
            )
        assert exc_info.value.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
