import pytest

from fastapi.exceptions import HTTPException
from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.endpoints.lib.helpers import simulate_updates_and_check_references
from nmdc_runtime.api.models.query import UpdateCommand, UpdateStatement
from starlette import status
from tests.lib.faker import Faker


@pytest.fixture()
def seeded_db():
    r"""Pytest fixture that yields a seeded database."""

    # Seed the database with the following interrelated documents (represented
    # here as a Mermaid graph/flowchart within a Markdown fenced code block):
    # Docs: https://mermaid.js.org/syntax/flowchart.html
    r"""
    ```mermaid
    graph BT
        study_a
        study_b --> |part_of| study_a
        biosample_a --> |associated_studies| study_a
        biosample_b --> |associated_studies| study_b
    ```
    """
    faker = Faker()
    study_a, study_b = faker.generate_studies(2)
    study_a["name"] = "Study A"
    study_b["name"] = "Study B"
    study_b["part_of"] = study_a["id"]
    study_ids = [study_a["id"], study_b["id"]]
    bsm_a, bsm_b = faker.generate_biosamples(2, associated_studies=[study_a["id"]])
    bsm_a["name"] = "Biosample A"
    bsm_b["name"] = "Biosample B"
    bsm_b["associated_studies"] = [study_b["id"]]  # overrides the default
    biosample_ids = [bsm_a["id"], bsm_b["id"]]
    
    mdb = get_mongo_db()
    assert mdb["study_set"].count_documents({"id": {"$in": study_ids}}) == 0
    assert mdb["biosample_set"].count_documents({"id": {"$in": biosample_ids}}) == 0
    mdb["study_set"].insert_many([study_a, study_b])
    mdb["biosample_set"].insert_many([bsm_a, bsm_b])
    
    yield mdb
    
    # 🧹 Clean up.
    mdb["study_set"].delete_many({"id": {"$in": study_ids}})
    mdb["biosample_set"].delete_many({"id": {"$in": biosample_ids}})


class TestSimulateUpdatesAndCheckReferences:
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

    def test_it_aborts_when_breaking_incoming_reference_from_other_collection(self, seeded_db):
        referrer_id = seeded_db["biosample_set"].find_one({"name": "Biosample A"})["id"]
        with pytest.raises(HTTPException) as exc_info:
            simulate_updates_and_check_references(
                db=seeded_db,
                update_cmd=UpdateCommand(
                    update="study_set",
                    updates=[
                        UpdateStatement(q={"name": "Study A"}, u={"$set": {"id": "nmdc:sty-00-000099"}}),
                    ],
                ),
            )
        assert exc_info.value.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
        assert "biosample_set" in exc_info.value.detail
        assert referrer_id in exc_info.value.detail

    def test_it_aborts_when_breaking_incoming_reference_from_same_collection(self, seeded_db):
        # Delete the referring biosample, so its breaking doesn't interfere with our assertion.
        referee_id = seeded_db["study_set"].find_one({"name": "Study A"})["id"]
        seeded_db["biosample_set"].delete_many({"associated_studies": referee_id})

        referrer_id = seeded_db["study_set"].find_one({"name": "Study B"})["id"]
        with pytest.raises(HTTPException) as exc_info:
            simulate_updates_and_check_references(
                db=seeded_db,
                update_cmd=UpdateCommand(
                    update="study_set",
                    updates=[
                        UpdateStatement(q={"name": "Study A"}, u={"$set": {"id": "nmdc:sty-00-000099"}}),
                    ],
                ),
            )
        assert exc_info.value.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
        assert "study_set" in exc_info.value.detail
        assert referrer_id in exc_info.value.detail

    def test_it_aborts_when_adding_broken_outgoing_reference_to_same_collection(self, seeded_db):
        referrer_id = seeded_db["study_set"].find_one({"name": "Study A"})["id"]
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
        assert "study_set" in exc_info.value.detail
        assert referrer_id in exc_info.value.detail

    def test_it_aborts_when_adding_broken_outgoing_reference_to_other_collection(self, seeded_db):
        assert seeded_db["biosample_set"].count_documents({}) == 2
        referrer_id = seeded_db["biosample_set"].find_one({"name": "Biosample A"})["id"]
        with pytest.raises(HTTPException) as exc_info:
            simulate_updates_and_check_references(
                db=seeded_db,
                update_cmd=UpdateCommand(
                    update="biosample_set",
                    updates=[
                        UpdateStatement(q={"name": "Biosample A"}, u={"$set": {"associated_studies": ["nmdc:sty-00-000099"]}}),
                    ],
                ),
            )
        assert exc_info.value.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
        assert "biosample_set" in exc_info.value.detail
        assert referrer_id in exc_info.value.detail
