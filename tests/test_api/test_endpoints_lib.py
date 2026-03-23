import pytest

from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.endpoints.lib.helpers import simulate_updates_and_check_references
from nmdc_runtime.api.endpoints.lib.workflow_executions import prepare_supersession_chain_for_workflow_execution_deletion
from nmdc_runtime.api.models.query import UpdateCommand, UpdateStatement
from tests.lib.faker import Faker


# TODO: Since `tests/conftest.py` already has a `seeded_db` fixture, consider renaming this fixture.
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
    def test_it_returns_empty_list_when_operation_does_not_break_any_references(self, seeded_db):
        assert simulate_updates_and_check_references(
            db=seeded_db,
            update_cmd=UpdateCommand(
                update="study_set",
                updates=[
                    UpdateStatement(q={"name": "Study A"}, u={"$set": {"name": "Study Alpha"}}),
                ],
            ),
        ) == []

    def test_it_returns_empty_list_when_operation_repairs_the_references_it_breaks(self, seeded_db):
        r"""
        In this test, we break a reference (by updating the referee's `id`) and then
        fix that newly-broken reference (by updating the reference accordingly), all
        within the context of a single command.
        """
        # Remind the reader about the initial state of things.
        new_study_id = "nmdc:sty-00-000099"
        assert seeded_db["study_set"].count_documents({"id": new_study_id}) == 0
        study_a = seeded_db["study_set"].find_one({"name": "Study A"})
        study_b = seeded_db["study_set"].find_one({"name": "Study B"})
        assert study_a["id"] in study_b["part_of"]

        # Delete the referring biosample, so its breaking doesn't interfere with our assertion.
        seeded_db["biosample_set"].delete_many({"associated_studies": study_a["id"]})        

        # Submit an update command that breaks the reference and then repairs it.
        assert simulate_updates_and_check_references(
            db=seeded_db,
            update_cmd=UpdateCommand(
                update="study_set",
                updates=[
                    UpdateStatement(q={"name": "Study A"}, u={"$set": {"id": new_study_id}}),
                    UpdateStatement(q={"name": "Study B"}, u={"$set": {"part_of": new_study_id}}),
                ],
            ),
        ) == []

    def test_it_returns_error_messages_when_breaking_incoming_references(self, seeded_db):
        referrer_id = seeded_db["biosample_set"].find_one({"name": "Biosample A"})["id"]
        error_messages = simulate_updates_and_check_references(
            db=seeded_db,
            update_cmd=UpdateCommand(
                update="study_set",
                updates=[
                    UpdateStatement(q={"name": "Study A"}, u={"$set": {"id": "nmdc:sty-00-000099"}}),
                ],
            ),
        )
        assert len(error_messages) == 2
        assert "biosample_set" in error_messages[0]
        assert referrer_id in error_messages[0]

    def test_it_returns_error_messages_when_breaking_incoming_reference_from_other_collection_only(self, seeded_db):
        # Delete the referring study, so we can focus on other-collection references.
        referee_id = seeded_db["study_set"].find_one({"name": "Study A"})["id"]
        seeded_db["study_set"].delete_many({"part_of": referee_id})

        referrer_id = seeded_db["biosample_set"].find_one({"name": "Biosample A"})["id"]
        error_messages = simulate_updates_and_check_references(
            db=seeded_db,
            update_cmd=UpdateCommand(
                update="study_set",
                updates=[
                    UpdateStatement(q={"name": "Study A"}, u={"$set": {"id": "nmdc:sty-00-000099"}}),
                ],
            ),
        )
        assert len(error_messages) == 1
        assert "biosample_set" in error_messages[0]
        assert referrer_id in error_messages[0]

    def test_it_returns_error_messages_when_breaking_incoming_reference_from_same_collection_only(self, seeded_db):
        # Delete the referring biosample, so we can focus on same-collection references.
        referee_id = seeded_db["study_set"].find_one({"name": "Study A"})["id"]
        seeded_db["biosample_set"].delete_many({"associated_studies": referee_id})

        referrer_id = seeded_db["study_set"].find_one({"name": "Study B"})["id"]
        error_messages = simulate_updates_and_check_references(
            db=seeded_db,
            update_cmd=UpdateCommand(
                update="study_set",
                updates=[
                    UpdateStatement(q={"name": "Study A"}, u={"$set": {"id": "nmdc:sty-00-000099"}}),
                ],
            ),
        )
        assert len(error_messages) == 1
        assert "study_set" in error_messages[0]
        assert referrer_id in error_messages[0]

    def test_it_returns_error_messages_when_adding_broken_outgoing_reference_to_same_collection(self, seeded_db):
        referrer_id = seeded_db["study_set"].find_one({"name": "Study A"})["id"]
        error_messages = simulate_updates_and_check_references(
            db=seeded_db,
            update_cmd=UpdateCommand(
                update="study_set",
                updates=[
                    UpdateStatement(q={"name": "Study A"}, u={"$set": {"part_of": "nmdc:sty-00-000099"}}),
                ],
            ),
        )
        assert len(error_messages) == 1
        assert "study_set" in error_messages[0]
        assert referrer_id in error_messages[0]

    def test_it_returns_error_messages_when_adding_broken_outgoing_reference_to_other_collection(self, seeded_db):
        referrer_id = seeded_db["biosample_set"].find_one({"name": "Biosample A"})["id"]
        error_messages = simulate_updates_and_check_references(
            db=seeded_db,
            update_cmd=UpdateCommand(
                update="biosample_set",
                updates=[
                    UpdateStatement(q={"name": "Biosample A"}, u={"$set": {"associated_studies": ["nmdc:sty-00-000099"]}}),
                ],
            ),
        )
        assert len(error_messages) == 1
        assert "biosample_set" in error_messages[0]
        assert referrer_id in error_messages[0]

    def test_it_returns_error_messages_when_document_having_new_id_has_broken_reference(self, seeded_db):
        r"""
        In this test, we update the `id` of a document _and_ give it a broken outgoing reference.
        This is to demonstrate that the function checks the referential integrity of documents whose
        `id`s have been updated (as of today, the function uses the document's `_id` value to keep track
        of the document across the update).
        """
        old_bsm_id = seeded_db["biosample_set"].find_one({"name": "Biosample A"})["id"]
        new_bsm_id = "nmdc:bsm-00-000099"
        nonexistent_study_id = "nmdc:sty-00-000099"
        assert seeded_db["biosample_set"].count_documents({"id": new_bsm_id}) == 0  # no such document
        assert seeded_db["study_set"].count_documents({"id": nonexistent_study_id}) == 0  # no such document
        error_messages = simulate_updates_and_check_references(
            db=seeded_db,
            update_cmd=UpdateCommand(
                update="biosample_set",
                updates=[
                    UpdateStatement(q={"name": "Biosample A"}, u={"$set": {"associated_studies": [nonexistent_study_id]}}),
                    UpdateStatement(q={"name": "Biosample A"}, u={"$set": {"id": new_bsm_id}}),
                ],
            ),
        )
        assert len(error_messages) == 1
        assert "biosample_set" in error_messages[0]
        assert old_bsm_id in error_messages[0]


class TestPrepareSupersessionChainForWorkflowExecutionDeletion:
    @pytest.fixture()
    def db_having_supersession_chains(self):
        """
        Fixture that seeds the database with the `superseded_by` relationships depicted below...

        ```mermaid
        graph
            wfe_a --> wfe_b
            wfe_b --> wfe_c
            dobj_a1 --> wfe_b
            dobj_a2 --> wfe_b
            dobj_b1 --> wfe_c
            dobj_b2 --> wfe_c
        ```
        
        ...yields the database, the 3 `WorkflowExecution`s, and the 4 `DataObject`s;
        and then cleans up the database.
        """

        # Generate fake `WorkflowExecution` documents.
        faker = Faker()
        wfe_a, wfe_b, wfe_c = faker.generate_metagenome_annotations(3, was_informed_by=["foo"], has_input=["bar"])
        wfe_a["superseded_by"] = wfe_b["id"]
        wfe_b["superseded_by"] = wfe_c["id"]

        # Generate fake `DataObject` documents.
        dobj_a1, dobj_a2 = faker.generate_data_objects(2, superseded_by=wfe_b["id"])
        dobj_b1, dobj_b2 = faker.generate_data_objects(2, superseded_by=wfe_c["id"])
        
        # Insert the documents into the database.
        db = get_mongo_db()
        wfe_ids = [wfe_a["id"], wfe_b["id"], wfe_c["id"]]
        assert db["workflow_execution_set"].count_documents({"id": {"$in": wfe_ids}}) == 0
        db["workflow_execution_set"].insert_many([wfe_a, wfe_b, wfe_c])
        dobj_ids = [dobj_a1["id"], dobj_a2["id"], dobj_b1["id"], dobj_b2["id"]]
        assert db["data_object_set"].count_documents({"id": {"$in": dobj_ids}}) == 0
        db["data_object_set"].insert_many([dobj_a1, dobj_a2, dobj_b1, dobj_b2])
        
        # Yield the seeded database—and relevant documents—to the dependent test.
        yield db, (wfe_a, wfe_b, wfe_c), (dobj_a1, dobj_a2, dobj_b1, dobj_b2)

        # Clean up.
        db["workflow_execution_set"].delete_many({"id": {"$in": wfe_ids}})
        db["data_object_set"].delete_many({"id": {"$in": dobj_ids}})

    def test_when_subject_wfe_supersedes_nothing(self, db_having_supersession_chains):
        db, (wfe_a, _, _), _ = db_having_supersession_chains
        fn = prepare_supersession_chain_for_workflow_execution_deletion  # concise alias
        fn(workflow_execution=wfe_a, db=db)  # nothing to assert; at least no exception was raised

    def test_when_subject_wfe_supersedes_something_and_is_superseded_by_something(self, db_having_supersession_chains):
        db, (wfe_a, wfe_b, wfe_c), (dobj_a1, dobj_a2, _, _) = db_having_supersession_chains
        fn = prepare_supersession_chain_for_workflow_execution_deletion  # concise alias
        fn(workflow_execution=wfe_b, db=db)
        assert db["workflow_execution_set"].find_one({"id": wfe_a["id"]})["superseded_by"] == wfe_c["id"]
        assert db["data_object_set"].find_one({"id": dobj_a1["id"]})["superseded_by"] == wfe_c["id"]
        assert db["data_object_set"].find_one({"id": dobj_a2["id"]})["superseded_by"] == wfe_c["id"]

    def test_when_subject_wfe_supersedes_something_and_is_superseded_by_nothing(self, db_having_supersession_chains):
        db, (_, wfe_b, wfe_c), (_, _, dobj_b1, dobj_b2) = db_having_supersession_chains
        fn = prepare_supersession_chain_for_workflow_execution_deletion  # concise alias
        fn(workflow_execution=wfe_c, db=db)
        assert "superseded_by" not in db["workflow_execution_set"].find_one({"id": wfe_b["id"]})
        assert "superseded_by" not in db["data_object_set"].find_one({"id": dobj_b1["id"]})
        assert "superseded_by" not in db["data_object_set"].find_one({"id": dobj_b2["id"]})
